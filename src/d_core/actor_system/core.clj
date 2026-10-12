(ns d-core.actor-system.core
  (:require [d-core.actor-system.client :as client]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.runtime.engine :as engine]
            [d-core.actor-system.storage.in-memory :as in-mem-storage]
            [d-core.actor-system.transport.in-memory :as in-mem-transport]))

(defn create-client
  "Creates an ActorClient from a transport."
  ([transport]
   (client/create-client transport {}))
  ([transport options]
   (client/create-client transport options)))

(defn tell!
  "Sends a fire-and-forget message to an entity actor."
  ([client entity-type entity-id message]
   (tell! client entity-type entity-id message {}))
  ([client entity-type entity-id message opts]
   (p/tell! client entity-type entity-id message opts)))

(defn ask!
  "Sends a request message and returns a future resolving to the reply."
  ([client entity-type entity-id message]
   (ask! client entity-type entity-id message {}))
  ([client entity-type entity-id message opts]
   (p/ask! client entity-type entity-id message opts)))

(defn ask-sync!
  "Sends a request message and blocks waiting for the reply."
  ([client entity-type entity-id message]
   (client/ask-sync! client entity-type entity-id message {}))
  ([client entity-type entity-id message opts]
   (client/ask-sync! client entity-type entity-id message opts)))

(defn- resolve-engine [runtime-or-engine]
  (or (:engine runtime-or-engine) runtime-or-engine))

(defn trigger-tick!
  "Synchronously dispatches a `{:type :tick}` message to all active actors or a specific entity actor."
  ([runtime-or-engine]
   (engine/trigger-tick! (resolve-engine runtime-or-engine)))
  ([runtime-or-engine entity-type entity-id]
   (engine/trigger-tick! (resolve-engine runtime-or-engine) entity-type entity-id)))

(defn sweep-idle-actors!
  "Deterministically runs one passivation sweep at `(time/now-ms)` and returns passivated entity keys."
  [runtime-or-engine]
  (engine/sweep-idle-actors! (resolve-engine runtime-or-engine)))

(defn flush-actor!
  "Synchronously flushes an active actor's state to its SnapshotStore."
  [runtime-or-engine entity-type entity-id]
  (engine/flush-actor! (resolve-engine runtime-or-engine) entity-type entity-id))

(defn- subscribe-all-entities!
  [transport engine entities options]
  (reduce (fn [subs [entity-type _spec]]
            (let [sub (p/subscribe-entities! transport
                                             entity-type
                                             (fn [type id envelope]
                                               (engine/dispatch-envelope! engine type id envelope))
                                             options)]
              (conj subs sub)))
          []
          entities))

(defn start-runtime
  "Starts an ActorRuntime worker host.
   Config:
   - :transport (ActorTransport implementation, defaults to in-memory)
   - :storage   (ActorSnapshotStore implementation, defaults to in-memory)
   - :entities  (map of {entity-type entity-spec})
   - :producer  (optional ProducerProtocol for :publish effects)
   - :db        (optional database handle for custom effects)
   - :effect-handler (optional fn [effect ctx] for custom effects)
   - :options   (e.g. :default-idle-timeout-ms, :watchdog-interval-ms, :partition, :partitions)"
  [{:keys [transport storage entities producer db effect-handler options]}]
  (let [actual-storage (or storage (in-mem-storage/create-in-memory-snapshot-store))
        actual-transport (or transport (in-mem-transport/create-in-memory-transport))
        effect-ctx (merge {:transport actual-transport
                           :storage actual-storage
                           :producer producer
                           :db db
                           :effect-handler effect-handler}
                          (:effect-ctx options))
        engine-opts (assoc (or options {}) :effect-ctx effect-ctx)
        engine (engine/create-actor-engine entities actual-storage engine-opts)
        subs (subscribe-all-entities! actual-transport engine entities (or options {}))]
    {:engine engine
     :transport actual-transport
     :storage actual-storage
     :entities entities
     :subscriptions subs
     :stop! (fn []
              (doseq [s subs]
                (try
                  (p/unsubscribe! actual-transport s {})
                  (catch Throwable _t nil)))
              (engine/stop-actor-engine! engine))}))

(defn stop-runtime!
  "Gracefully stops the runtime, unsubscribes, saves snapshots, and stops actors."
  [runtime]
  (when-let [stop-fn (:stop! runtime)]
    (stop-fn)))
