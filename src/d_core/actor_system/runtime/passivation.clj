(ns d-core.actor-system.runtime.passivation
  (:require [d-core.actor-system.runtime.actor :as actor]))

(defn find-idle-actors
  "Pure function: filters active actors map for entities that have exceeded their idle timeout."
  [active-actors-map now-ms default-timeout-ms]
  (reduce-kv
   (fn [acc entity-key actor-instance]
     (let [idle-timeout (get-in actor-instance [:spec :idle-timeout-ms] default-timeout-ms)
           last-active (actor/last-active-at actor-instance)
           elapsed (- now-ms (or last-active now-ms))]
       (if (>= elapsed idle-timeout)
         (conj acc entity-key)
         acc)))
   []
   active-actors-map))

(defn scan-and-passivate!
  "Executes one pass of the passivation scan against the runtime active actors."
  [active-atom passivate-fn! default-timeout-ms]
  (let [now (System/currentTimeMillis)
        active @active-atom
        idle-keys (find-idle-actors active now default-timeout-ms)]
    (doseq [[entity-type entity-id] idle-keys]
      (try
        (passivate-fn! entity-type entity-id {})
        (catch Throwable _t nil)))
    idle-keys))
