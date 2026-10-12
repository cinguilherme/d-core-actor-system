(ns d-core.actor-system.storage.sql-adapter
  "ActorSnapshotStore implementation backed by d-core-db SimpleSQLProtocol (`upsert!` / `select`).
   Works identically across SQLite, PostgreSQL, and MySQL."
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [d-core.actor-system.protocols :as p]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.libs.time :as time]))

(defn- encode-state [state fmt]
  (case (or fmt :edn)
    :json (json/generate-string state)
    (pr-str state)))

(defn- decode-state [raw fmt]
  (when (some? raw)
    (cond
      (map? raw) raw
      (string? raw) (case (or fmt :edn)
                      :json (json/parse-string raw true)
                      (edn/read-string raw))
      :else raw)))

(defn- row-val [row k]
  (or (get row k)
      (some (fn [[rk rv]]
              (when (= (name rk) (name k))
                rv))
            row)))

(defrecord SqlSnapshotStore [db options]
  p/ActorSnapshotStore
  (load-snapshot [_ entity-type entity-id _opts]
    (let [table (get options :table :actor_snapshots)
          fmt (get options :format :edn)
          rows (sql/select db {:table table
                               :where {:entity_type (name entity-type)
                                       :entity_id (str entity-id)}
                               :limit 1})]
      (when-let [row (first rows)]
        (decode-state (row-val row :state_payload) fmt))))

  (save-snapshot! [_ entity-type entity-id state opts]
    (let [table (get options :table :actor_snapshots)
          fmt (get options :format :edn)
          payload (encode-state state fmt)
          seq-val (some-> (:sequence opts) str)
          row (cond-> {:entity_type (name entity-type)
                       :entity_id (str entity-id)
                       :state_payload payload
                       :updated_at (time/now-ms)}
                seq-val (assoc :sequence_num seq-val))]
      (sql/upsert! db row {:table table
                           :conflict-columns [:entity_type :entity_id]})
      state)))

(defn create-sql-snapshot-store
  "Creates a `SqlSnapshotStore` backed by any `SimpleSQLProtocol` implementation."
  ([db]
   (create-sql-snapshot-store db {}))
  ([db options]
   (->SqlSnapshotStore db (or options {}))))
