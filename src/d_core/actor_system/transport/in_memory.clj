(ns d-core.actor-system.transport.in-memory
  (:require [d-core.actor-system.protocols :as p]))

(defrecord InMemoryTransport [handlers-atom]
  p/ActorTransport
  (send-message! [_ entity-type entity-id envelope _opts]
    (let [handlers (get @handlers-atom (keyword entity-type))]
      (if (seq handlers)
        (do
          (doseq [h (vals handlers)]
            (Thread/startVirtualThread #(h entity-type entity-id envelope)))
          true)
        (throw (ex-info (str "No subscriber registered for entity type: " entity-type)
                        {:entity-type entity-type :entity-id entity-id})))))

  (request-message! [_ entity-type entity-id envelope opts]
    (let [timeout-ms (get opts :timeout-ms 5000)
          p (promise)
          reply-fn (fn [reply] (deliver p reply))
          envelope-with-reply (assoc envelope :reply-fn reply-fn)
          handlers (get @handlers-atom (keyword entity-type))]
      (if (seq handlers)
        (do
          ;; Route to first available handler
          (let [h (first (vals handlers))]
            (Thread/startVirtualThread #(h entity-type entity-id envelope-with-reply)))
          (let [res (deref p timeout-ms {:error :timeout :timeout-ms timeout-ms})]
            (if (= :timeout (:error res))
              (throw (ex-info (str "Actor request timed out after " timeout-ms "ms")
                              {:entity-type entity-type :entity-id entity-id :timeout-ms timeout-ms}))
              res)))
        (throw (ex-info (str "No subscriber registered for entity type: " entity-type)
                        {:entity-type entity-type :entity-id entity-id})))))

  (subscribe-entities! [_ entity-type handler-fn _opts]
    (let [sub-id (str (java.util.UUID/randomUUID))]
      (swap! handlers-atom assoc-in [(keyword entity-type) sub-id] handler-fn)
      {:entity-type (keyword entity-type) :subscription-id sub-id}))

  (unsubscribe! [_ subscription _opts]
    (let [{:keys [entity-type subscription-id]} subscription]
      (swap! handlers-atom update entity-type dissoc subscription-id)
      true)))

(defn create-in-memory-transport
  "Creates an in-memory transport instance for local/monolith testing and execution."
  ([]
   (create-in-memory-transport (atom {})))
  ([handlers-atom]
   (->InMemoryTransport handlers-atom)))
