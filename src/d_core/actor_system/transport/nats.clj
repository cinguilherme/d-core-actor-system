(ns d-core.actor-system.transport.nats
  (:require [cheshire.core :as json]
            [d-core.actor-system.protocols :as p])
  (:import [io.nats.client Connection Message MessageHandler]
           [java.nio.charset StandardCharsets]
           [java.time Duration]))

(defn- entity-subject [prefix entity-type entity-id]
  (str prefix "." (name entity-type) "." (str entity-id)))

(defn- extract-entity-id [prefix entity-type subject]
  (let [expected-prefix (str prefix "." (name entity-type) ".")]
    (when (.startsWith ^String subject expected-prefix)
      (subs subject (count expected-prefix)))))

(defn- serialize-payload [data]
  (.getBytes (json/generate-string data) StandardCharsets/UTF_8))

(defn- deserialize-payload [^bytes data]
  (json/parse-string (String. data StandardCharsets/UTF_8) true))

(defrecord NatsTransport [^Connection conn options]
  p/ActorTransport
  (send-message! [_ entity-type entity-id envelope _opts]
    (let [prefix (get options :subject-prefix "actors")
          subject (entity-subject prefix entity-type entity-id)
          payload-bytes (serialize-payload envelope)]
      (.publish conn subject payload-bytes)
      true))

  (request-message! [_ entity-type entity-id envelope opts]
    (let [prefix (get options :subject-prefix "actors")
          timeout-ms (get opts :timeout-ms (get options :request-timeout-ms 5000))
          subject (entity-subject prefix entity-type entity-id)
          payload-bytes (serialize-payload envelope)
          ^Message reply-msg (.request conn subject payload-bytes (Duration/ofMillis timeout-ms))]
      (if reply-msg
        (deserialize-payload (.getData reply-msg))
        (throw (ex-info (str "NATS request timed out after " timeout-ms "ms")
                        {:entity-type entity-type :entity-id entity-id :timeout-ms timeout-ms})))))

  (subscribe-entities! [_ entity-type handler-fn opts]
    (let [prefix (get options :subject-prefix "actors")
          queue-group (get opts :queue-group (get options :queue-group "actor-workers"))
          wildcard-subject (str prefix "." (name entity-type) ".*")
          dispatcher (.createDispatcher conn
                                        (reify MessageHandler
                                          (^void onMessage [_ ^Message msg]
                                            (try
                                              (let [subj (.getSubject msg)
                                                    e-id (extract-entity-id prefix entity-type subj)
                                                    raw-data (.getData msg)
                                                    envelope (deserialize-payload raw-data)
                                                    reply-to (.getReplyTo msg)
                                                    envelope-with-reply (if (and reply-to (not (.isEmpty reply-to)))
                                                                          (assoc envelope :reply-fn
                                                                                 (fn [reply-data]
                                                                                   (.publish conn reply-to (serialize-payload reply-data))))
                                                                          envelope)]
                                                (handler-fn entity-type e-id envelope-with-reply))
                                              (catch Throwable _t nil)))))]
      (if queue-group
        (.subscribe dispatcher wildcard-subject queue-group)
        (.subscribe dispatcher wildcard-subject))
      {:dispatcher dispatcher :subject wildcard-subject}))

  (unsubscribe! [_ subscription _opts]
    (when-let [^io.nats.client.Dispatcher disp (:dispatcher subscription)]
      (.closeDispatcher conn disp)
      true)))

(defn create-nats-transport
  "Creates an ActorTransport backed by an active NATS Connection."
  ([^Connection conn]
   (create-nats-transport conn {}))
  ([^Connection conn options]
   (->NatsTransport conn options)))
