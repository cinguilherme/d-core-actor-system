(ns d-core.actor-system.runtime.actor
  (:require [d-core.actor-system.behavior :as behavior]
            [d-core.actor-system.protocols :as p])
  (:import [java.util.concurrent LinkedTransferQueue]))

(defn- build-actor-context
  [entity-type entity-id]
  {:entity-type entity-type
   :entity-id entity-id
   :now (System/currentTimeMillis)})

(defn- persist-snapshot-if-needed!
  [snapshot-store entity-type entity-id spec state count]
  (when (and snapshot-store (behavior/should-snapshot? spec count))
    (try
      (p/save-snapshot! snapshot-store entity-type entity-id state {})
      (catch Throwable _t
        nil))))

(defn- process-message-envelope!
  [actor envelope]
  (let [{:keys [entity-type entity-id spec snapshot-store state-atom]} actor
        {:keys [payload reply-fn context]} envelope
        current-state (:state @state-atom)
        ctx (merge (build-actor-context entity-type entity-id) context)
        step-result (behavior/execute-step spec current-state payload ctx)
        next-state (:next-state step-result)
        reply (:reply step-result)]
    (swap! state-atom (fn [curr]
                        (let [new-count (inc (get curr :messages-processed 0))]
                          (assoc curr
                                 :state next-state
                                 :messages-processed new-count
                                 :last-active-at (System/currentTimeMillis)))))
    (when reply-fn
      (try
        (reply-fn reply)
        (catch Throwable _t nil)))
    (persist-snapshot-if-needed! snapshot-store
                                 entity-type
                                 entity-id
                                 spec
                                 next-state
                                 (:messages-processed @state-atom))))

(defn- handle-envelope!
  [actor envelope]
  (case (:type envelope)
    :stop
    (let [{:keys [reply-fn]} envelope]
      (swap! (:state-atom actor) assoc :status :stopping)
      (when reply-fn
        (reply-fn {:ok true}))
      :break)

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
        ;; Save final state upon exit
        (when snapshot-store
          (try
            (p/save-snapshot! snapshot-store entity-type entity-id (:state @state-atom) {})
            (catch Throwable _t nil)))
        (swap! state-atom assoc :status :stopped)))))

(defn create-actor
  "Spawns a new Virtual Thread actor instance."
  [entity-type entity-id spec snapshot-store initial-state]
  (let [mailbox (LinkedTransferQueue.)
        state-atom (atom {:state initial-state
                          :messages-processed 0
                          :last-active-at (System/currentTimeMillis)
                          :status :running})
        actor {:entity-type entity-type
               :entity-id entity-id
               :spec spec
               :snapshot-store snapshot-store
               :mailbox mailbox
               :state-atom state-atom}
        thread (Thread/startVirtualThread #(actor-loop actor))]
    (assoc actor :thread thread)))

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

(defn get-state
  "Returns current state snapshot immediately from memory."
  [actor]
  (:state @(:state-atom actor)))

(defn last-active-at
  "Returns timestamp of last message processed."
  [actor]
  (:last-active-at @(:state-atom actor)))

(defn active?
  "Checks if the actor is actively running."
  [actor]
  (= :running (:status @(:state-atom actor))))
