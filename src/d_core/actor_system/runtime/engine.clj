(ns d-core.actor-system.runtime.engine
  (:require [d-core.actor-system.behavior :as behavior]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.runtime.actor :as actor]
            [d-core.actor-system.runtime.passivation :as pass])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.function Function]))

(defn- entity-key [entity-type entity-id]
  [(keyword entity-type) (str entity-id)])

(defn- activate-actor!
  [entity-type entity-id entity-spec snapshot-store]
  (let [snapshot (when snapshot-store
                   (p/load-snapshot snapshot-store entity-type entity-id {}))
        initial-state (behavior/initialize-state entity-spec entity-id snapshot)]
    (actor/create-actor entity-type entity-id entity-spec snapshot-store initial-state)))

(defrecord ActorEngine [entity-specs snapshot-store active-map watchdog-stop-p options]
  p/ActorRuntime
  (active-actors [_ _opts]
    (reduce (fn [acc [k act]]
              (assoc acc k {:entity-type (:entity-type act)
                            :entity-id (:entity-id act)
                            :status (:status @(:state-atom act))
                            :messages-processed (:messages-processed @(:state-atom act))
                            :last-active-at (:last-active-at @(:state-atom act))}))
            {}
            active-map))

  (passivate-entity! [_ entity-type entity-id _opts]
    (let [k (entity-key entity-type entity-id)
          act (.remove ^ConcurrentHashMap active-map k)]
      (if act
        (do
          (actor/stop-actor! act (get options :stop-timeout-ms 5000))
          true)
        false)))

  (get-actor-state [_ entity-type entity-id _opts]
    (when-let [act (.get ^ConcurrentHashMap active-map (entity-key entity-type entity-id))]
      (actor/get-state act))))

(defn get-or-activate!
  "Retrieves the active actor or activates a new one using atomic computeIfAbsent."
  [^ActorEngine engine entity-type entity-id]
  (let [k (entity-key entity-type entity-id)
        spec (get (:entity-specs engine) (keyword entity-type))]
    (when-not spec
      (throw (ex-info (str "Unknown entity type: " entity-type)
                      {:entity-type entity-type})))
    (.computeIfAbsent ^ConcurrentHashMap (:active-map engine)
                      k
                      (reify Function
                        (apply [_ _]
                          (activate-actor! entity-type entity-id spec (:snapshot-store engine)))))))

(defn dispatch-envelope!
  "Dispatches an envelope to an entity actor, activating it if necessary."
  [^ActorEngine engine entity-type entity-id envelope]
  (let [act (get-or-activate! engine entity-type entity-id)]
    (actor/send-envelope! act envelope)))

(defn- start-passivation-watchdog!
  [^ConcurrentHashMap active-map passivate-fn default-timeout-ms interval-ms stop-p]
  (Thread/startVirtualThread
   (fn []
     (loop []
       (let [stopped? (deref stop-p interval-ms nil)]
         (when-not stopped?
           (let [now (System/currentTimeMillis)
                 idle-keys (pass/find-idle-actors active-map now default-timeout-ms)]
             (doseq [[e-type e-id] idle-keys]
               (try
                 (passivate-fn e-type e-id {})
                 (catch Throwable _t nil))))
           (recur)))))))

(defn create-actor-engine
  "Creates and initializes an ActorEngine with entity specs and snapshot store."
  ([entity-specs]
   (create-actor-engine entity-specs nil {}))
  ([entity-specs snapshot-store]
   (create-actor-engine entity-specs snapshot-store {}))
  ([entity-specs snapshot-store options]
   (doseq [[_ spec] entity-specs]
     (behavior/validate-entity-spec! spec))
   (let [active-map (ConcurrentHashMap.)
         stop-p (promise)
         default-idle-timeout (get options :default-idle-timeout-ms 300000)
         interval-ms (get options :watchdog-interval-ms 1000)
         engine (->ActorEngine entity-specs snapshot-store active-map stop-p options)]
     (start-passivation-watchdog! active-map
                                 (fn [e-type e-id opts]
                                   (p/passivate-entity! engine e-type e-id opts))
                                 default-idle-timeout
                                 interval-ms
                                 stop-p)
     engine)))

(defn stop-actor-engine!
  "Stops the watchdog and drains/stops all active actors."
  [^ActorEngine engine]
  (deliver (:watchdog-stop-p engine) true)
  (let [active-map ^ConcurrentHashMap (:active-map engine)
        keys (vec (.keySet active-map))]
    (doseq [k keys]
      (when-let [act (.remove active-map k)]
        (actor/stop-actor! act (get-in engine [:options :stop-timeout-ms] 5000))))
    true))
