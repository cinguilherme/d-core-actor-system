(ns d-core.actor-system.client
  (:require [d-core.actor-system.protocols :as p]
            [d-core.libs.time :as time]))

(defn- build-envelope
  [payload opts]
  (cond-> {:type :message
           :payload payload
           :context (:context opts)
           :timestamp (time/now-ms)}
    (:sequence opts) (assoc :sequence (:sequence opts))))

(defrecord DefaultActorClient [transport options]
  p/ActorClient
  (tell! [_ entity-type entity-id message opts]
    (let [envelope (build-envelope message opts)]
      (p/send-message! transport entity-type entity-id envelope opts)))

  (ask! [_ entity-type entity-id message opts]
    (let [merged-opts (merge options opts)
          envelope (build-envelope message merged-opts)]
      ;; Returns a future so callers can deref or wait asynchronously
      (future
        (p/request-message! transport entity-type entity-id envelope merged-opts)))))

(defn create-client
  "Creates an ActorClient backed by the given ActorTransport."
  ([transport]
   (create-client transport {}))
  ([transport options]
   (->DefaultActorClient transport options)))

(defn ask-sync!
  "Convenience helper to ask and synchronously block for the result."
  ([client entity-type entity-id message]
   (ask-sync! client entity-type entity-id message {}))
  ([client entity-type entity-id message opts]
   @(p/ask! client entity-type entity-id message opts)))
