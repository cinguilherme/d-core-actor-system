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
   - :options   (e.g. :default-idle-timeout-ms, :watchdog-interval-ms)"
  [{:keys [transport storage entities options]}]
  (let [actual-storage (or storage (in-mem-storage/create-in-memory-snapshot-store))
        actual-transport (or transport (in-mem-transport/create-in-memory-transport))
        engine (engine/create-actor-engine entities actual-storage options)
        subs (subscribe-all-entities! actual-transport engine entities options)]
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
