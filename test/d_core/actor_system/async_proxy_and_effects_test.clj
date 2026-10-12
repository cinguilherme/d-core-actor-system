(ns d-core.actor-system.async-proxy-and-effects-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.core :as actor-core]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.storage.sql-adapter :as sql-adapter]
            [d-core.actor-system.transport.async-proxy :as ap-transport]
            [d-core.core.clients.sqlite.client :as sqlite-client]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.core.databases.sqlite]
            [d-core.core.producers.common :as common-prod]
            [d-core.core.producers.in-memory :as im-prod]
            [d-core.libs.time :as time]
            [d-core-async-proxy.grpc.server :as grpc-srv]
            [d-core-async-proxy.subscriptions.registry :as reg]
            [integrant.core :as ig]
            [next.jdbc :as jdbc]))

(deftest effects-and-controllable-clock-test
  (testing "Child-to-parent :tell effect, custom effect handler, trigger-tick!, and deterministic passivation via ControllableClock"
    (let [clk (time/manual-clock 1000000)
          custom-effects (atom [])]
      (time/with-clock clk
        (let [entities {:child {:init-fn (fn [id] {:id id :count 0 :ticks 0})
                                :idle-timeout-ms 5000
                                :snapshot-every-n 100
                                :step-fn (fn [state msg _ctx]
                                           (case (:type msg)
                                             :increment
                                             (let [n (inc (:count state))]
                                               [(assoc state :count n)
                                                {:count n}
                                                [{:type :tell
                                                  :entity-type :parent
                                                  :entity-id "agg-1"
                                                  :message {:type :child-delta
                                                            :child-id (:id state)
                                                            :delta 1}}
                                                 {:type :audit-log
                                                  :child-id (:id state)
                                                  :new-count n}]])

                                             :tick
                                             [(update state :ticks inc)
                                              {:ticks (inc (:ticks state))}
                                              []]))}
                        :parent {:init-fn (fn [id] {:id id :total 0 :by-child {}})
                                 :idle-timeout-ms 60000
                                 :step-fn (fn [state msg _ctx]
                                            (case (:type msg)
                                              :child-delta
                                              [(-> state
                                                   (update :total + (:delta msg))
                                                   (update-in [:by-child (:child-id msg)] (fnil + 0) (:delta msg)))
                                               {:ok true}]

                                              :get-total
                                              [state {:total (:total state)
                                                      :by-child (:by-child state)}]))}}
              runtime (actor-core/start-runtime
                       {:entities entities
                        :effect-handler (fn [eff _ctx]
                                          (swap! custom-effects conj eff))
                        :options {:watchdog-interval-ms 60000}})
              client (actor-core/create-client (:transport runtime))]
          (try
            (is (= {:count 1} (actor-core/ask-sync! client :child "c-1" {:type :increment})))
            (is (= {:count 2} (actor-core/ask-sync! client :child "c-1" {:type :increment})))
            (is (= {:count 1} (actor-core/ask-sync! client :child "c-2" {:type :increment})))

            ;; Wait briefly for async child->parent :tell effects to settle
            (Thread/sleep 50)
            (is (= {:total 3 :by-child {"c-1" 2 "c-2" 1}}
                   (actor-core/ask-sync! client :parent "agg-1" {:type :get-total})))
            (is (= 3 (count @custom-effects)))

            ;; Deterministic tick trigger
            (let [tick-res (actor-core/trigger-tick! runtime :child "c-1")]
              (is (= {:ticks 1} tick-res)))

            ;; Advance manual clock by 6000ms (> child's 5000ms idle timeout, < parent's 60000ms)
            (time/advance-clock! clk 6000)
            (let [passivated (set (actor-core/sweep-idle-actors! runtime))]
              (is (contains? passivated [:child "c-1"]))
              (is (contains? passivated [:child "c-2"]))
              (is (not (contains? passivated [:parent "agg-1"]))))

            ;; Re-activating child "c-1" hydrates from snapshot saved on passivation!
            (is (= {:ticks 2} (actor-core/trigger-tick! runtime :child "c-1")))
            (finally
              (actor-core/stop-runtime! runtime))))))))

(deftest sql-snapshot-store-test
  (testing "SqlSnapshotStore persists and reloads snapshots via SimpleSQLProtocol upsert!/select"
    (let [tmp-file (java.io.File/createTempFile "actor-snapshots-" ".db")
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
        (let [store (sql-adapter/create-sql-snapshot-store db)]
          (is (nil? (p/load-snapshot store :precinct "p-10" {})))
          (p/save-snapshot! store :precinct "p-10"
                            {:tallies {:alice 10 :bob 8} :voters #{"v1" "v2"}}
                            {:sequence "seq-100"})
          (is (= {:tallies {:alice 10 :bob 8} :voters #{"v1" "v2"}}
                 (p/load-snapshot store :precinct "p-10" {})))
          ;; Upsert overwrite
          (p/save-snapshot! store :precinct "p-10"
                            {:tallies {:alice 15 :bob 9} :voters #{"v1" "v2" "v3"}}
                            {:sequence "seq-105"})
          (is (= {:tallies {:alice 15 :bob 9} :voters #{"v1" "v2" "v3"}}
                 (p/load-snapshot store :precinct "p-10" {})))
          (is (= 1 (count (sql/select db {:table :actor_snapshots})))))
        (finally
          (.delete tmp-file))))))

(deftest distributed-async-proxy-sharded-actor-workers-test
  (testing "Multi-worker ActorRuntime over AsyncProxyTransport (gRPC) with deterministic sharding and child->parent aggregation"
    (let [in-proc-name (str "actor-proxy-" (random-uuid))
          queues (ig/init-key :d-core.queue/in-memory-queues {:topics [:default]})
          im-producer (im-prod/->InMemoryProducer queues nil)
          routing {:publish {:default {:targets :in-memory}}}
          producer (common-prod/->CommonProducer :in-memory {:in-memory im-producer} routing nil nil)
          registry (reg/make-registry {:queues queues
                                       :dispatcher-ctx {:max-attempts 2 :timeout-ms 2000}
                                       :logger nil})
          proxy-server (grpc-srv/start-grpc-server {:producer producer
                                                    :registry registry
                                                    :transport :in-process
                                                    :in-process-name in-proc-name})
          entities {:counter {:init-fn (fn [id] {:id id :val 0})
                              :step-fn (fn [state msg _ctx]
                                         (case (:op msg)
                                           :add
                                           (let [nv (+ (:val state) (:n msg))]
                                             [(assoc state :val nv)
                                              {:id (:id state) :val nv :worker (:worker state)}])
                                           :get
                                           [state {:id (:id state) :val (:val state)}]))}}
          t-worker-0 (ap-transport/create-async-proxy-transport
                      {:transport :in-process
                       :in-process-name in-proc-name
                       :partition 0
                       :partitions 2})
          t-worker-1 (ap-transport/create-async-proxy-transport
                      {:transport :in-process
                       :in-process-name in-proc-name
                       :partition 1
                       :partitions 2})
          t-ingress (ap-transport/create-async-proxy-transport
                     {:transport :in-process
                      :in-process-name in-proc-name
                      :partitions 2})
          rt-0 (actor-core/start-runtime {:transport t-worker-0
                                          :entities entities
                                          :options {:partition 0 :partitions 2}})
          rt-1 (actor-core/start-runtime {:transport t-worker-1
                                          :entities entities
                                          :options {:partition 1 :partitions 2}})
          ingress-client (actor-core/create-client t-ingress)]
      (try
        (doseq [idx (range 6)]
          (let [eid (str "counter-" idx)
                r1 (actor-core/ask-sync! ingress-client :counter eid {:op :add :n 10} {:timeout-ms 2000})
                r2 (actor-core/ask-sync! ingress-client :counter eid {:op :add :n 5} {:timeout-ms 2000})]
            (is (= 10 (:val r1)))
            (is (= 15 (:val r2)))))
        ;; Verify each counter actor lives on exactly ONE of the two workers (single-writer guarantee)
        (let [active-0 (set (keys (p/active-actors (:engine rt-0) {})))
              active-1 (set (keys (p/active-actors (:engine rt-1) {})))]
          (is (empty? (set/intersection active-0 active-1)))
          (is (= 6 (+ (count active-0) (count active-1)))))
        (finally
          (actor-core/stop-runtime! rt-0)
          (actor-core/stop-runtime! rt-1)
          (ap-transport/close-async-proxy-transport! t-worker-0)
          (ap-transport/close-async-proxy-transport! t-worker-1)
          (ap-transport/close-async-proxy-transport! t-ingress)
          (ig/halt-key! :d-core-async-proxy.grpc/server proxy-server)
          (ig/halt-key! :d-core-async-proxy.subscriptions/registry registry))))))
