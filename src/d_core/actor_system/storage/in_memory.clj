(ns d-core.actor-system.storage.in-memory
  (:require [d-core.actor-system.protocols :as p]))

(defrecord InMemorySnapshotStore [storage-atom]
  p/ActorSnapshotStore
  (load-snapshot [_ entity-type entity-id _opts]
    (get-in @storage-atom [(keyword entity-type) (str entity-id)]))

  (save-snapshot! [_ entity-type entity-id state _opts]
    (swap! storage-atom assoc-in [(keyword entity-type) (str entity-id)] state)
    state))

(defn create-in-memory-snapshot-store
  "Creates an in-memory snapshot store backed by an atom."
  ([]
   (create-in-memory-snapshot-store (atom {})))
  ([initial-state-atom]
   (->InMemorySnapshotStore initial-state-atom)))
