(ns d-core.actor-system.runtime.actor
  (:require [d-core.actor-system.behavior :as behavior]
            [d-core.actor-system.protocols :as p]
            [d-core.core.metrics.tracing :as otel]
            [d-core.core.metrics.wrappers :as wrappers]
            [d-core.core.producers.protocol :as prod]
            [d-core.libs.time :as time])
  (:import [java.util.concurrent LinkedTransferQueue]))

(defn- build-actor-context
  [entity-type entity-id]
  {:entity-type entity-type
   :entity-id entity-id
   :now (time/now-ms)
   :instant (time/now)})

(defn- record-snapshot-metric!
  [{:keys [metrics actor-instruments]} entity-type]
  (when (and metrics actor-instruments)
    (wrappers/record-actor-snapshot! metrics actor-instruments entity-type)))

(defn- persist-snapshot-if-needed!
  [snapshot-store entity-type entity-id spec state count last-seq effect-ctx]
  (when (and snapshot-store (behavior/should-snapshot? spec count))
    (try
      (p/save-snapshot! snapshot-store entity-type entity-id state
                        (cond-> {:messages-processed count}
                          last-seq (assoc :sequence last-seq)))
      (record-snapshot-metric! effect-ctx entity-type)
      (catch Throwable _t
        nil))))

(defn- extract-effect-map
  [effect]
  (cond
    (map? effect) effect
    (vector? effect) (let [[t a b c] effect]
                       (case t
                         :tell {:type :tell :entity-type a :entity-id b :message c}
                         :publish {:type :publish :topic a :message b :options c}
                         {:type t :args (vec (rest effect))}))
    :else {:type effect}))

(defn- execute-single-effect!
  [{:keys [entity-type entity-id spec snapshot-store effect-ctx]} next-state effect]
  (let [eff-map (extract-effect-map effect)
        eff-type (:type eff-map)
        {:keys [metrics actor-instruments]} effect-ctx
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
                            (or (:opts eff-map) {}))
          (record-snapshot-metric! effect-ctx entity-type))

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
      (when (and metrics actor-instruments)
        (wrappers/record-actor-effect! metrics actor-instruments eff-type :ok))
      (catch Throwable _t
        (when (and metrics actor-instruments)
          (wrappers/record-actor-effect! metrics actor-instruments eff-type :error))
        nil))))

(defn- execute-effects!
  [actor next-state effects]
  (when (seq effects)
    (doseq [eff effects]
      (execute-single-effect! actor next-state eff))))

(defn- extract-message-op
  [payload]
  (if (map? payload)
    (or (:type payload) (:op payload) :message)
    :message))

(defn- process-message-envelope!
  [actor envelope]
  (let [now (time/now-ms)
        start-nanos (System/nanoTime)
        {:keys [entity-type entity-id spec snapshot-store state-atom effect-ctx]} actor
        {:keys [metrics actor-instruments]} effect-ctx
        {:keys [payload reply-fn context sequence metadata produced-at pulled-at received-at enqueued-at]} envelope
        op (extract-message-op payload)
        carrier (or metadata (:metadata payload) context)
        mailbox-wait-ms (if enqueued-at (max 0 (- now (long enqueued-at))) 0)
        broker-transit-ms (if (and produced-at pulled-at) (max 0 (- (long pulled-at) (long produced-at))) 0)
        proxy-transit-ms (if (and pulled-at received-at) (max 0 (- (long received-at) (long pulled-at))) 0)
        total-transit-ms (if produced-at (max 0 (- now (long produced-at))) 0)]
    (otel/with-span [nil "actor.step_execute"
                     {:carrier carrier
                      :kind :internal
                      :attributes {"entity.type" (name entity-type)
                                   "entity.id" (str entity-id)
                                   "op" (name op)
                                   "transit_lag_ms" total-transit-ms
                                   "mailbox_wait_ms" mailbox-wait-ms
                                   "broker_transit_ms" broker-transit-ms
                                   "proxy_transit_ms" proxy-transit-ms}}]
      (let [current-state (:state @state-atom)
            ctx (merge (build-actor-context entity-type entity-id)
                       context
                       (when sequence {:sequence sequence}))
            step-result (behavior/execute-step spec current-state payload ctx)
            next-state (:next-state step-result)
            reply (:reply step-result)
            effects (:effects step-result)
            tick-msg? (= :tick op)
            eff-produced-at (or produced-at (:timestamp envelope))
            step-status (if (and (map? reply) (= :rejected (:status reply)))
                          :rejected
                          :ok)
            updated-meta (swap! state-atom
                                (fn [curr]
                                  (let [new-count (inc (get curr :messages-processed 0))]
                                    (cond-> (assoc curr
                                                   :state next-state
                                                   :messages-processed new-count
                                                   :last-active-at now)
                                      sequence (assoc :last-sequence sequence)
                                      tick-msg? (assoc :last-tick-at now)))))]
        (when (and metrics actor-instruments)
          (wrappers/record-actor-step! metrics actor-instruments entity-type op step-status
                                       (wrappers/duration-seconds start-nanos))
          (when (and (not tick-msg?) (number? eff-produced-at))
            (wrappers/record-actor-transit-lag!
             metrics actor-instruments entity-type op
             (/ (double (max 0 (- now (long eff-produced-at)))) 1000.0))))
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
                                     (:last-sequence updated-meta)
                                     effect-ctx)))))

(defn- handle-flush-envelope!
  [{:keys [entity-type entity-id snapshot-store state-atom effect-ctx]} {:keys [reply-fn]}]
  (let [curr @state-atom]
    (when snapshot-store
      (try
        (p/save-snapshot! snapshot-store entity-type entity-id (:state curr)
                          (cond-> {:messages-processed (:messages-processed curr)}
                            (:last-sequence curr) (assoc :sequence (:last-sequence curr))))
        (record-snapshot-metric! effect-ctx entity-type)
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
  (let [{:keys [mailbox state-atom snapshot-store entity-type entity-id effect-ctx]} actor
        {:keys [metrics actor-instruments]} effect-ctx]
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
                                  (:last-sequence curr) (assoc :sequence (:last-sequence curr))))
              (record-snapshot-metric! effect-ctx entity-type))
            (catch Throwable _t nil)))
        (when (and metrics actor-instruments)
          (wrappers/record-actor-stop! metrics actor-instruments entity-type))
        (swap! state-atom assoc :status :stopped)))))

(defn create-actor
  "Spawns a new Virtual Thread actor instance."
  ([entity-type entity-id spec snapshot-store initial-state]
   (create-actor entity-type entity-id spec snapshot-store initial-state {}))
  ([entity-type entity-id spec snapshot-store initial-state effect-ctx]
   (let [mailbox (LinkedTransferQueue.)
         now (time/now-ms)
         eff-ctx (or effect-ctx {})
         {:keys [metrics actor-instruments]} eff-ctx
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
                :effect-ctx eff-ctx
                :mailbox mailbox
                :state-atom state-atom}
         _ (when (and metrics actor-instruments)
             (wrappers/record-actor-spawn! metrics actor-instruments entity-type))
         thread (Thread/startVirtualThread (bound-fn [] (actor-loop actor)))]
     (assoc actor :thread thread))))

(defn send-envelope!
  "Puts an envelope into the actor mailbox."
  [actor envelope]
  (let [env-to-send (if (contains? envelope :enqueued-at)
                      envelope
                      (assoc envelope :enqueued-at (time/now-ms)))]
    (.offer ^LinkedTransferQueue (:mailbox actor) env-to-send)))

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
