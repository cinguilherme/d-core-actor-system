(ns d-core.actor-system.storage.store-adapter
  (:require [cheshire.core :as json]
            [d-core.actor-system.protocols :as p]
            [d-core.core.storage.protocol :as sp]))

(defn- storage-key [prefix entity-type entity-id]
  (str prefix "/" (name entity-type) "/" (str entity-id) ".json"))

(defrecord StorageProtocolAdapter [storage-backend options]
  p/ActorSnapshotStore
  (load-snapshot [_ entity-type entity-id opts]
    (let [prefix (get options :prefix "actors")
          k (storage-key prefix entity-type entity-id)]
      (try
        (when-let [raw (sp/storage-get storage-backend k opts)]
          (cond
            (map? raw) raw
            (string? raw) (json/parse-string raw true)
            :else raw))
        (catch Throwable _t nil))))

  (save-snapshot! [_ entity-type entity-id state opts]
    (let [prefix (get options :prefix "actors")
          k (storage-key prefix entity-type entity-id)
          payload (json/generate-string state)]
      (sp/storage-put storage-backend k payload opts)
      state)))

(defn create-storage-adapter
  "Adapts any d-core StorageProtocol implementation to ActorSnapshotStore."
  ([storage-backend]
   (create-storage-adapter storage-backend {}))
  ([storage-backend options]
   (->StorageProtocolAdapter storage-backend options)))
