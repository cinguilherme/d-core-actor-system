(ns d-core.actor-system.runtime.engine
  (:require [d-core.actor-system.behavior :as behavior]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.runtime.actor :as actor]
            [d-core.actor-system.runtime.passivation :as pass]
            [d-core.libs.time :as time])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.function Function]))

(defn- entity-key [entity-type entity-id]
  [(keyword entity-type) (str entity-id)])

(defn- activate-actor!
  [entity-type entity-id entity-spec snapshot-store effect-ctx]
  (let [snapshot (when snapshot-store
                   (p/load-snapshot snapshot-store entity-type entity-id {}))
        initial-state (behavior/initialize-state entity-spec entity-id snapshot)]
    (actor/create-actor entity-type entity-id entity-spec snapshot-store initial-state effect-ctx)))

(defrecord ActorEngine [entity-specs snapshot-store active-map watchdog-stop-p effect-ctx-atom options]
  p/ActorRuntime
  (active-actors [_ _opts]
    (reduce (fn [acc [k act]]
              (let [st @(:state-atom act)]
                (assoc acc k {:entity-type (:entity-type act)
                              :entity-id (:entity-id act)
                              :status (:status st)
                              :messages-processed (:messages-processed st)
                              :last-sequence (:last-sequence st)
                              :last-active-at (:last-active-at st)})))
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
                          (activate-actor! entity-type
                                           entity-id
                                           spec
                                           (:snapshot-store engine)
                                           @(:effect-ctx-atom engine)))))))

(defn dispatch-envelope!
  "Dispatches an envelope to an entity actor, activating it if necessary."
  [^ActorEngine engine entity-type entity-id envelope]
  (let [act (get-or-activate! engine entity-type entity-id)]
    (actor/send-envelope! act envelope)))

(defn sweep-idle-actors!
  "Deterministically runs one passivation sweep at `(time/now-ms)` and returns passivated entity keys."
  [^ActorEngine engine]
  (let [now (time/now-ms)
        default-timeout-ms (get-in engine [:options :default-idle-timeout-ms] 300000)
        idle-keys (pass/find-idle-actors (:active-map engine) now default-timeout-ms)]
    (doseq [[e-type e-id] idle-keys]
      (try
        (p/passivate-entity! engine e-type e-id {})
        (catch Throwable _t nil)))
    idle-keys))

(defn- dispatch-due-ticks!
  [^ConcurrentHashMap active-map now-ms]
  (doseq [[_k act] active-map]
    (when-let [tick-ms (get-in act [:spec :tick-interval-ms])]
      (let [last-tick (or (actor/last-tick-at act) 0)]
        (when (>= (- now-ms (long last-tick)) (long tick-ms))
          (actor/send-envelope! act {:type :message
                                     :payload {:type :tick :timestamp now-ms}
                                     :timestamp now-ms}))))))

(defn trigger-tick!
  "Synchronously sends a `{:type :tick}` message to a specific active actor or all active actors,
   awaiting completion for deterministic testing."
  ([^ActorEngine engine]
   (let [now (time/now-ms)
         acts (vec (vals (:active-map engine)))
         promises (mapv (fn [act]
                          (let [pr (promise)]
                            (actor/send-envelope! act {:type :message
                                                       :payload {:type :tick :timestamp now}
                                                       :timestamp now
                                                       :reply-fn #(deliver pr %)})
                            pr))
                        acts)]
     (mapv #(deref % 5000 {:timeout true}) promises)))
  ([^ActorEngine engine entity-type entity-id]
   (let [act (get-or-activate! engine entity-type entity-id)
         now (time/now-ms)
         pr (promise)]
     (actor/send-envelope! act {:type :message
                                :payload {:type :tick :timestamp now}
                                :timestamp now
                                :reply-fn #(deliver pr %)})
     (deref pr 5000 {:timeout true}))))

(defn flush-actor!
  "Synchronously flushes an active actor's state to its SnapshotStore."
  [^ActorEngine engine entity-type entity-id]
  (when-let [act (.get ^ConcurrentHashMap (:active-map engine) (entity-key entity-type entity-id))]
    (actor/flush-actor! act (get-in engine [:options :stop-timeout-ms] 5000))))

(defn- start-passivation-watchdog!
  [^ActorEngine engine interval-ms stop-p]
  (Thread/startVirtualThread
   (bound-fn []
     (loop []
       (let [stopped? (deref stop-p interval-ms nil)]
         (when-not stopped?
           (let [now (time/now-ms)]
             (dispatch-due-ticks! (:active-map engine) now)
             (sweep-idle-actors! engine))
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
         interval-ms (get options :watchdog-interval-ms 1000)
         effect-ctx-atom (atom (:effect-ctx options {}))
         engine (->ActorEngine entity-specs snapshot-store active-map stop-p effect-ctx-atom options)]
     (swap! effect-ctx-atom assoc
            :engine engine
            :dispatch-fn (fn [t id env] (dispatch-envelope! engine t id env)))
     (start-passivation-watchdog! engine interval-ms stop-p)
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
