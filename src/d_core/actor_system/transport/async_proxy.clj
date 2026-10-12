(ns d-core.actor-system.transport.async-proxy
  "Distributed ActorTransport implementation backed by d-core-async-proxy over gRPC.
   Provides deterministic key-based partition sharding (`entity-id -> partition`)
   and bridges both asynchronous `tell!` and synchronous `ask!` (`RequestReply`)."
  (:require [d-core.actor-system.protocols :as p]
            [d-core.core.consumers.async-proxy :as ap-cons]
            [d-core.core.producers.async-proxy :as ap-prod]
            [d-core.core.producers.protocol :as prod]))

(defn entity-topic
  "Constructs the topic keyword for an entity type, e.g. `:actor.precinct`."
  [topic-prefix entity-type]
  (let [prefix (or topic-prefix "actor")]
    (keyword (str (name prefix) "." (name entity-type)))))

(defn- handle-proxy-delivery
  [handler-fn delivery-env step-timeout-ms]
  (let [payload (:payload delivery-env)
        e-type (keyword (or (:entity-type payload) :unknown))
        e-id (str (or (:entity-id payload) (:key delivery-env)))
        raw-env (or (:envelope payload) {:type :message :payload payload})
        produced-at (or (:produced-at delivery-env)
                        (get-in delivery-env [:metadata :timestamp])
                        (:timestamp raw-env)
                        (:timestamp delivery-env))
        reply-p (promise)
        env-with-reply (cond-> (assoc raw-env
                                      :sequence (:delivery-id delivery-env)
                                      :reply-fn #(deliver reply-p %))
                         produced-at (assoc :produced-at produced-at))]
    (handler-fn e-type e-id env-with-reply)
    (let [reply-val (deref reply-p (long (or step-timeout-ms 5000)) ::timeout)]
      (if (= reply-val ::timeout)
        {:disposition :retry
         :error "Timed out waiting for local actor step completion"}
        {:disposition :ack
         :reply reply-val}))))

(defrecord AsyncProxyTransport [grpc-client producer owns-client? options]
  p/ActorTransport
  (send-message! [_ entity-type entity-id envelope opts]
    (let [merged (merge options opts)
          topic (entity-topic (:topic-prefix merged) entity-type)
          msg {:entity-type (keyword entity-type)
               :entity-id (str entity-id)
               :envelope (dissoc envelope :reply-fn)}]
      (prod/produce! producer msg
                     (cond-> {:topic topic
                              :key (str entity-id)}
                       (:partitions merged) (assoc :partitions (:partitions merged))
                       (:target-partition opts) (assoc :partition (:target-partition opts))))
      true))

  (request-message! [_ entity-type entity-id envelope opts]
    (let [merged (merge options opts)
          timeout-ms (long (or (:timeout-ms merged) 5000))
          topic (entity-topic (:topic-prefix merged) entity-type)
          msg {:entity-type (keyword entity-type)
               :entity-id (str entity-id)
               :envelope (dissoc envelope :reply-fn)}
          resp (ap-prod/request-reply!
                producer
                msg
                (cond-> {:topic topic
                         :key (str entity-id)
                         :timeout-ms timeout-ms}
                  (:partitions merged) (assoc :partitions (:partitions merged))
                  (:target-partition opts) (assoc :partition (:target-partition opts))))]
      (case (:status resp)
        :ok (:reply resp)
        :timeout (throw (ex-info (str "Actor request timed out after " timeout-ms "ms")
                                 {:entity-type entity-type
                                  :entity-id entity-id
                                  :timeout-ms timeout-ms}))
        (throw (ex-info (or (:error resp) "Actor request failed over async-proxy")
                        {:entity-type entity-type
                         :entity-id entity-id
                         :response resp})))))

  (subscribe-entities! [_ entity-type handler-fn opts]
    (let [merged (merge options opts)
          topic (entity-topic (:topic-prefix merged) entity-type)
          part (:partition merged)
          parts (:partitions merged)
          sub-id (keyword (str "actor-" (name entity-type)
                               (when (some? part) (str "-p" part))
                               "-" (subs (str (random-uuid)) 0 8)))
          mode (or (:mode merged) (:delivery-mode merged) :stream)
          step-timeout-ms (or (:step-timeout-ms merged) 5000)
          consumer (ap-cons/start-consumer
                    {:grpc-client grpc-client
                     :subscriptions
                     {sub-id (cond-> {:subscription-id sub-id
                                      :topic topic
                                      :mode mode
                                      :options (merge {:poll-ms (or (:poll-ms merged) 10)}
                                                      (:subscription-options merged))
                                      :handler #(handle-proxy-delivery handler-fn % step-timeout-ms)}
                               (some? part) (assoc :partition (long part))
                               (some? parts) (assoc :partitions (long parts)))}})]
      {:entity-type (keyword entity-type)
       :subscription-id sub-id
       :consumer consumer}))

  (unsubscribe! [_ subscription _opts]
    (when-let [consumer (:consumer subscription)]
      (ap-cons/stop-consumer! consumer))
    true))

(defn create-async-proxy-transport
  "Creates an `AsyncProxyTransport` instance talking to `d-core-async-proxy` over gRPC.
   Options:
   - `:grpc-client` (optional pre-configured GrpcClient for `AsyncProxyService`)
   - `:host` / `:port` / `:target` / `:transport` / `:in-process-name`
   - `:topic-prefix` (default \"actor\")
   - `:partition` (worker shard index `0..N-1`)
   - `:partitions` (total shard count `N`)
   - `:mode` (`:stream` default, or `:callback`)"
  ([]
   (create-async-proxy-transport {}))
  ([{:keys [grpc-client partitions] :as options}]
   (let [owns? (nil? grpc-client)
         client (or grpc-client (ap-prod/make-grpc-client options))
         producer (ap-prod/make-producer {:grpc-client client
                                          :default-partitions partitions})]
     (->AsyncProxyTransport client producer owns? (or options {})))))

(defn close-async-proxy-transport!
  [^AsyncProxyTransport transport]
  (when (and transport (:owns-client? transport))
    (ap-prod/close-producer! (:producer transport))))
