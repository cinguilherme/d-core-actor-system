(ns d-core.actor-system.system
  (:require [d-core.actor-system.client :as client]
            [d-core.actor-system.core :as core]
            [d-core.actor-system.storage.in-memory :as in-mem-storage]
            [d-core.actor-system.storage.sql-adapter :as sql-adapter]
            [d-core.actor-system.storage.store-adapter :as store-adapter]
            [d-core.actor-system.transport.async-proxy :as ap-transport]
            [d-core.actor-system.transport.in-memory :as in-mem-transport]
            [d-core.actor-system.transport.nats :as nats-transport]
            [integrant.core :as ig]))

;; Storage Component
(defmethod ig/init-key :d-core.actor-system/storage
  [_ {:keys [type initial-state backend db options]}]
  (case type
    :in-memory (in-mem-storage/create-in-memory-snapshot-store (or initial-state (atom {})))
    :storage-protocol (store-adapter/create-storage-adapter backend (or options {}))
    :sql (sql-adapter/create-sql-snapshot-store (or db backend) (or options {}))
    (in-mem-storage/create-in-memory-snapshot-store (atom {}))))

;; Transport Component
(defmethod ig/init-key :d-core.actor-system/transport
  [_ {:keys [type nats-connection options] :as cfg}]
  (case type
    :nats (nats-transport/create-nats-transport nats-connection options)
    :async-proxy (ap-transport/create-async-proxy-transport
                  (merge (dissoc cfg :type) (or options {})))
    :in-memory (in-mem-transport/create-in-memory-transport)
    (in-mem-transport/create-in-memory-transport)))

(defmethod ig/halt-key! :d-core.actor-system/transport
  [_ transport]
  (when (instance? d_core.actor_system.transport.async_proxy.AsyncProxyTransport transport)
    (ap-transport/close-async-proxy-transport! transport)))

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
