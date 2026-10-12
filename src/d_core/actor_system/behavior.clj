(ns d-core.actor-system.behavior
  (:require [malli.core :as m]
            [malli.error :as me]))

(def EntitySpecSchema
  [:map
   [:init-fn fn?]
   [:step-fn fn?]
   [:idle-timeout-ms {:optional true} pos-int?]
   [:snapshot-every-n {:optional true} pos-int?]
   [:tick-interval-ms {:optional true} pos-int?]
   [:effect-handler {:optional true} fn?]])

(def default-options
  {:idle-timeout-ms 300000       ;; 5 minutes
   :snapshot-every-n 50})

(defn validate-entity-spec!
  "Validates that the provided entity-spec conforms to EntitySpecSchema."
  [spec]
  (when-not (m/validate EntitySpecSchema spec)
    (let [explanation (me/humanize (m/explain EntitySpecSchema spec))]
      (throw (ex-info "Invalid entity spec definition"
                      {:spec spec
                       :errors explanation})))))

(defn initialize-state
  "Initializes the actor's working state.
   Uses snapshot if present; otherwise invokes the spec's :init-fn."
  [entity-spec entity-id snapshot]
  (if (some? snapshot)
    snapshot
    (let [init-fn (or (:init-fn entity-spec) (constantly {}))]
      (init-fn entity-id))))

(defn- normalize-step-result
  "Normalizes the output of step-fn into a standard map.
   Supports [next-state reply] or [next-state reply effects]."
  [result current-state]
  (cond
    (and (vector? result) (= 3 (count result)))
    {:next-state (nth result 0)
     :reply (nth result 1)
     :effects (nth result 2)}

    (and (vector? result) (= 2 (count result)))
    {:next-state (nth result 0)
     :reply (nth result 1)
     :effects []}

    ;; If only next-state is returned directly as a map/value
    (not (vector? result))
    {:next-state result
     :reply nil
     :effects []}

    :else
    {:next-state current-state
     :reply nil
     :effects []
     :error "Invalid step result format"}))

(defn execute-step
  "Executes a pure step transition for the given state and message.
   Catches any exceptions and returns a failure result map without corrupting state."
  [entity-spec state message context]
  (let [step-fn (:step-fn entity-spec)]
    (try
      (let [result (step-fn state message context)]
        (normalize-step-result result state))
      (catch Throwable t
        {:next-state state
         :reply nil
         :effects []
         :error (.getMessage t)
         :exception t}))))

(defn should-snapshot?
  "Determines whether a snapshot should be taken based on message counter and spec."
  [entity-spec message-count]
  (let [threshold (get entity-spec :snapshot-every-n (:snapshot-every-n default-options))]
    (and (pos? threshold)
         (zero? (mod message-count threshold)))))
