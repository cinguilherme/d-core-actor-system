(ns d-core.actor-system.system
  (:require [d-core.actor-system.client :as client]
            [d-core.actor-system.core :as core]
            [d-core.actor-system.storage.in-memory :as in-mem-storage]
            [d-core.actor-system.storage.store-adapter :as store-adapter]
            [d-core.actor-system.transport.in-memory :as in-mem-transport]
            [d-core.actor-system.transport.nats :as nats-transport]
            [integrant.core :as ig]))

;; Storage Component
(defmethod ig/init-key :d-core.actor-system/storage
  [_ {:keys [type initial-state backend options]}]
  (case type
    :in-memory (in-mem-storage/create-in-memory-snapshot-store (or initial-state (atom {})))
    :storage-protocol (store-adapter/create-storage-adapter backend (or options {}))
    (in-mem-storage/create-in-memory-snapshot-store (atom {}))))

;; Transport Component
(defmethod ig/init-key :d-core.actor-system/transport
  [_ {:keys [type nats-connection options]}]
  (case type
    :nats (nats-transport/create-nats-transport nats-connection options)
    :in-memory (in-mem-transport/create-in-memory-transport)
    (in-mem-transport/create-in-memory-transport)))

;; Client Component
(defmethod ig/init-key :d-core.actor-system/client
  [_ {:keys [transport options]}]
  (client/create-client transport (or options {})))

;; Runtime Component
(defmethod ig/init-key :d-core.actor-system/runtime
  [_ config]
  (core/start-runtime config))

(defmethod ig/halt-key! :d-core.actor-system/runtime
  [_ runtime]
  (core/stop-runtime! runtime))
