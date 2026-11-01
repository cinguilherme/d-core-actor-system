(ns d-core.actor-system.resiliency-gaps-test
  "Tests exposing resiliency and lifecycle defects in `d-core-actor-system`:
   1. gRPC `ManagedChannel` leak when `close-async-proxy-transport!` is called on
      a transport that created its own `grpc-client`.
   2. Unfenced `SqlSnapshotStore` overwrite when an evicted/superseded actor
      stops after a new partition owner has already persisted a newer snapshot."
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.runtime.actor :as actor]
            [d-core.actor-system.storage.sql-adapter :as sql-adapter]
            [d-core.actor-system.transport.async-proxy :as ap-transport]
            [d-core.core.clients.sqlite.client :as sqlite-client]
            [d-core.core.databases.sqlite]
            [d-core.core.grpc :as grpc]
            [integrant.core :as ig]
            [next.jdbc :as jdbc])
  (:import [io.grpc ManagedChannel]))

(deftest close-async-proxy-transport-shuts-down-owned-grpc-channel
  (testing "When create-async-proxy-transport creates its own grpc-client, close-async-proxy-transport! must shut down the ManagedChannel"
    (let [transport (ap-transport/create-async-proxy-transport
                     {:transport :in-process
                      :in-process-name (str "leak-test-" (random-uuid))
                      :partition 0
                      :partitions 32})
          ^ManagedChannel ch (get-in transport [:grpc-client :channel])]
      (try
        (is (true? (:owns-client? transport)))
        (is (false? (.isShutdown ch)))
        (ap-transport/close-async-proxy-transport! transport)
        (is (true? (.isShutdown ch))
            "BUG: create-async-proxy-transport passes `:grpc-client client` to `ap-prod/make-producer`, setting `(:owns-client? producer)` to false, so `close-async-proxy-transport!` -> `close-producer!` never closes `(:grpc-client transport)` and leaks a ManagedChannel on every partition eviction/stop")
        (finally
          (when-not (.isShutdown ch)
            (grpc/close-client! (:grpc-client transport))))))))

(deftest evicted-actor-stop-must-not-clobber-newer-owner-snapshot
  (testing "Stopping an evicted actor on an old worker must not overwrite a newer snapshot written by the new partition owner"
    (let [tmp-file (java.io.File/createTempFile "actor-snapshot-fence-" ".db")
          jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath tmp-file))
          client (sqlite-client/make-client {:jdbc-url jdbc-url})
          db (ig/init-key :d-core.core.databases.sqlite/db {:sqlite-client client})]
      (try
        (jdbc/execute! (:datasource client)
                       ["CREATE TABLE actor_snapshots (
                           entity_type TEXT NOT NULL,
                           entity_id TEXT NOT NULL,
                           state_payload TEXT NOT NULL,
                           sequence_num TEXT,
                           updated_at INTEGER NOT NULL,
                           PRIMARY KEY (entity_type, entity_id)
                         )"])
        (let [store (sql-adapter/create-sql-snapshot-store db)
              spec {:init-fn (fn [id] {:id id :total-votes 0})
                    :step-fn (fn [state msg _ctx]
                               (case (:op msg)
                                 :vote [(update state :total-votes inc) {:ok true}]
                                 [state {:ok true}]))
                    :snapshot-every-n 1000}
              ;; Old owner (Worker A) activates actor at total-votes = 10
              old-actor (actor/create-actor :precinct "p-1" spec store {:id "p-1" :total-votes 10} {:fencing 1})]
          ;; Lease expires on Worker A; Worker B acquires lease (fencing = 2), processes votes up to 50,
          ;; and persists its snapshot to `actor_snapshots`.
          (p/save-snapshot! store :precinct "p-1" {:id "p-1" :total-votes 50} {:sequence "seq-50" :fencing 2})
          (is (= {:id "p-1" :total-votes 50} (p/load-snapshot store :precinct "p-1" {})))

          ;; Worker A now notices lease loss (`:lease-lost` / `:reconcile-mismatch`) and calls `evict!` -> `stop-actor!`.
          ;; `actor-loop`'s `finally` block (actor.clj:263-269) unconditionally saves its stale state (10) to `actor_snapshots`!
          (actor/stop-actor! old-actor 2000)

          (is (= {:id "p-1" :total-votes 50}
                 (p/load-snapshot store :precinct "p-1" {}))
              "BUG: `actor-loop` finally block and `SqlSnapshotStore/save-snapshot!` have no ownership/epoch fencing, so stopping an evicted actor overwrites the new owner's snapshot (50) with stale state (10)"))
        (finally
          (.delete tmp-file))))))
