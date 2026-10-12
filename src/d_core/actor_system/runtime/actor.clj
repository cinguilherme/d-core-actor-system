(ns d-core.actor-system.runtime.actor
  (:require [d-core.actor-system.behavior :as behavior]
            [d-core.actor-system.protocols :as p]
            [d-core.core.producers.protocol :as prod]
            [d-core.libs.time :as time])
  (:import [java.util.concurrent LinkedTransferQueue]))

(defn- build-actor-context
  [entity-type entity-id]
  {:entity-type entity-type
   :entity-id entity-id
   :now (time/now-ms)
   :instant (time/now)})

(defn- persist-snapshot-if-needed!
  [snapshot-store entity-type entity-id spec state count last-seq]
  (when (and snapshot-store (behavior/should-snapshot? spec count))
    (try
      (p/save-snapshot! snapshot-store entity-type entity-id state
                        (cond-> {:messages-processed count}
                          last-seq (assoc :sequence last-seq)))
      (catch Throwable _t
        nil))))

(defn- execute-single-effect!
  [{:keys [entity-type entity-id spec snapshot-store effect-ctx]} next-state effect]
  (let [eff-map (cond
                  (map? effect) effect
                  (vector? effect) (let [[t a b c] effect]
                                     (case t
                                       :tell {:type :tell :entity-type a :entity-id b :message c}
                                       :publish {:type :publish :topic a :message b :options c}
                                       {:type t :args (vec (rest effect))}))
                  :else {:type effect})
        eff-type (:type eff-map)
        custom-handler (or (:effect-handler spec) (:effect-handler effect-ctx))]
    (try
      (case eff-type
        :tell
        (let [target-type (:entity-type eff-map)
              target-id (:entity-id eff-map)
              msg (or (:message eff-map) (:payload eff-map))
              opts (or (:opts eff-map) {})]
          (cond
            (:client effect-ctx)
            (p/tell! (:client effect-ctx) target-type target-id msg opts)

            (:transport effect-ctx)
            (p/send-message! (:transport effect-ctx)
                             target-type
                             target-id
                             {:type :message
                              :payload msg
                              :timestamp (time/now-ms)}
                             opts)

            (:dispatch-fn effect-ctx)
            ((:dispatch-fn effect-ctx) target-type target-id
                                       {:type :message
                                        :payload msg
                                        :timestamp (time/now-ms)})))

        :publish
        (when-let [producer (:producer effect-ctx)]
          (let [topic (or (:topic eff-map) :default)
                msg (or (:message eff-map) (:payload eff-map))
                opts (merge {:topic topic} (:options eff-map))]
            (prod/produce! producer msg opts)))

        (:snapshot :flush-snapshot)
        (when snapshot-store
          (p/save-snapshot! snapshot-store entity-type entity-id next-state
                            (or (:opts eff-map) {})))

        (:fn :custom)
        (if-let [f (:fn eff-map)]
          (f {:entity-type entity-type
              :entity-id entity-id
              :state next-state
              :effect-ctx effect-ctx
              :effect eff-map})
          (when custom-handler
            (custom-handler eff-map {:entity-type entity-type
                                     :entity-id entity-id
                                     :state next-state
                                     :effect-ctx effect-ctx})))

        (when custom-handler
          (custom-handler eff-map {:entity-type entity-type
                                   :entity-id entity-id
                                   :state next-state
                                   :effect-ctx effect-ctx})))
      (catch Throwable _t
        nil))))

(defn- execute-effects!
  [actor next-state effects]
  (when (seq effects)
    (doseq [eff effects]
      (execute-single-effect! actor next-state eff))))

(defn- process-message-envelope!
  [actor envelope]
  (let [{:keys [entity-type entity-id spec snapshot-store state-atom]} actor
        {:keys [payload reply-fn context sequence]} envelope
        current-state (:state @state-atom)
        now (time/now-ms)
        ctx (merge (build-actor-context entity-type entity-id)
                   context
                   (when sequence {:sequence sequence}))
        step-result (behavior/execute-step spec current-state payload ctx)
        next-state (:next-state step-result)
        reply (:reply step-result)
        effects (:effects step-result)
        tick-msg? (and (map? payload) (= :tick (:type payload)))
        updated-meta (swap! state-atom
                            (fn [curr]
                              (let [new-count (inc (get curr :messages-processed 0))]
                                (cond-> (assoc curr
                                               :state next-state
                                               :messages-processed new-count
                                               :last-active-at now)
                                  sequence (assoc :last-sequence sequence)
                                  tick-msg? (assoc :last-tick-at now)))))]
    (execute-effects! actor next-state effects)
    (when reply-fn
      (try
        (reply-fn reply)
        (catch Throwable _t nil)))
    (persist-snapshot-if-needed! snapshot-store
                                 entity-type
                                 entity-id
                                 spec
                                 next-state
                                 (:messages-processed updated-meta)
                                 (:last-sequence updated-meta))))

(defn- handle-flush-envelope!
  [{:keys [entity-type entity-id snapshot-store state-atom]} {:keys [reply-fn]}]
  (let [curr @state-atom]
    (when snapshot-store
      (try
        (p/save-snapshot! snapshot-store entity-type entity-id (:state curr)
                          (cond-> {:messages-processed (:messages-processed curr)}
                            (:last-sequence curr) (assoc :sequence (:last-sequence curr))))
        (catch Throwable _t nil)))
    (when reply-fn
      (reply-fn {:ok true
                 :state (:state curr)
                 :last-sequence (:last-sequence curr)}))
    :continue))

(defn- handle-envelope!
  [actor envelope]
  (case (:type envelope)
    :stop
    (let [{:keys [reply-fn]} envelope]
      (swap! (:state-atom actor) assoc :status :stopping)
      (when reply-fn
        (reply-fn {:ok true}))
      :break)

    :flush
    (handle-flush-envelope! actor envelope)

    :message
    (do
      (process-message-envelope! actor envelope)
      :continue)))

(defn- actor-loop
  [actor]
  (let [{:keys [mailbox state-atom snapshot-store entity-type entity-id]} actor]
    (try
      (loop []
        (when (= :running (:status @state-atom))
          (let [envelope (.take ^LinkedTransferQueue mailbox)
                action (handle-envelope! actor envelope)]
            (when (= :continue action)
              (recur)))))
      (finally
        (when snapshot-store
          (try
            (let [curr @state-atom]
              (p/save-snapshot! snapshot-store entity-type entity-id (:state curr)
                                (cond-> {:messages-processed (:messages-processed curr)}
                                  (:last-sequence curr) (assoc :sequence (:last-sequence curr)))))
            (catch Throwable _t nil)))
        (swap! state-atom assoc :status :stopped)))))

(defn create-actor
  "Spawns a new Virtual Thread actor instance."
  ([entity-type entity-id spec snapshot-store initial-state]
   (create-actor entity-type entity-id spec snapshot-store initial-state {}))
  ([entity-type entity-id spec snapshot-store initial-state effect-ctx]
   (let [mailbox (LinkedTransferQueue.)
         now (time/now-ms)
         state-atom (atom {:state initial-state
                           :messages-processed 0
                           :last-active-at now
                           :last-tick-at now
                           :last-sequence nil
                           :status :running})
         actor {:entity-type entity-type
                :entity-id entity-id
                :spec spec
                :snapshot-store snapshot-store
                :effect-ctx (or effect-ctx {})
                :mailbox mailbox
                :state-atom state-atom}
         thread (Thread/startVirtualThread (bound-fn [] (actor-loop actor)))]
     (assoc actor :thread thread))))

(defn send-envelope!
  "Puts an envelope into the actor mailbox."
  [actor envelope]
  (.offer ^LinkedTransferQueue (:mailbox actor) envelope))

(defn stop-actor!
  "Sends a stop envelope and waits for actor termination up to timeout-ms."
  ([actor]
   (stop-actor! actor 5000))
  ([actor timeout-ms]
   (let [p (promise)]
     (send-envelope! actor {:type :stop :reply-fn #(deliver p %)})
     (deref p timeout-ms {:timeout true})
     (when-let [^Thread th (:thread actor)]
       (.join th timeout-ms))
     @(:state-atom actor))))

(defn flush-actor!
  "Sends a synchronous flush envelope to save the actor's snapshot and returns the result."
  ([actor]
   (flush-actor! actor 5000))
  ([actor timeout-ms]
   (let [p (promise)]
     (send-envelope! actor {:type :flush :reply-fn #(deliver p %)})
     (deref p timeout-ms {:timeout true}))))

(defn get-state
  "Returns current state snapshot immediately from memory."
  [actor]
  (:state @(:state-atom actor)))

(defn last-active-at
  "Returns timestamp of last message processed."
  [actor]
  (:last-active-at @(:state-atom actor)))

(defn last-tick-at
  "Returns timestamp of last tick processed."
  [actor]
  (:last-tick-at @(:state-atom actor)))

(defn active?
  "Checks if the actor is actively running."
  [actor]
  (= :running (:status @(:state-atom actor))))
