(ns d-core.actor-system.transport.async-proxy
  "Distributed ActorTransport implementation backed by d-core-async-proxy over gRPC.
   Provides deterministic key-based partition sharding (`entity-id -> partition`)
   and bridges both asynchronous `tell!` and synchronous `ask!` (`RequestReply`)."
  (:require [clojure.string :as str]
            [d-core.actor-system.protocols :as p]
            [d-core.core.consumers.async-proxy :as ap-cons]
            [d-core.core.producers.async-proxy :as ap-prod]
            [d-core.core.producers.protocol :as prod]
            [d-core.libs.time :as time]))

(defn entity-topic
  "Constructs the topic keyword for an entity type, e.g. `:actor.precinct`."
  [topic-prefix entity-type]
  (let [prefix (or topic-prefix "actor")]
    (keyword (str (name prefix) "." (name entity-type)))))

(defn- lane-topic
  "Applies a lane suffix to an entity topic: `:actor.precinct` + \".read\" ->
   `:actor.precinct.read`.

   The suffix rides on the topic, so the proxy partitions it exactly like the
   default lane (`actor.precinct.read.p5`) and gives it its own durable. That is
   what stops a read from queueing behind the ballot backlog."
  [topic suffix]
  (if (str/blank? (str suffix))
    topic
    (keyword (str (name topic) suffix))))

(defn- lane-sub-id
  "Subscription id for a lane. It labels queue-depth and in-flight-age gauges, so
   it has to be stable and distinct per lane rather than random."
  [entity-type partition suffix]
  (keyword (str "actor-" (name entity-type)
                (when-not (str/blank? (str suffix))
                  (str "-" (str/replace (str suffix) #"[^a-zA-Z0-9]+" "")))
                (when (some? partition) (str "-p" partition)))))

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
        pulled-at (:pulled-at delivery-env)
        received-at (time/now-ms)
        meta-info (merge (or (:metadata delivery-env) {})
                         (or (:metadata raw-env) {}))
        reply-expected? (boolean
                         (or (:reply-expected? meta-info)
                             (:reply-expected? delivery-env)
                             (:sync? delivery-env)
                             (:correlation-id delivery-env)
                             (:correlation-id meta-info)
                             (:reply-to delivery-env)
                             (:reply-to meta-info)))
        reply-p (when reply-expected? (promise))
        ;; Ownership epoch, stamped by the proxy when it enforced the lease.
        ;;
        ;; It must reach the STEP, not just the envelope: the only thing that
        ;; stops a former owner from overwriting the new owner's state is the
        ;; write being guarded by this value. The proxy cannot do that for us, and
        ;; the broker cannot either -- it has already delivered the message.
        fencing (:fencing delivery-env)
        env-with-reply (cond-> (assoc raw-env
                                      :sequence (:delivery-id delivery-env)
                                      :metadata meta-info
                                      :received-at received-at)
                         reply-p (assoc :reply-fn #(deliver reply-p %))
                         produced-at (assoc :produced-at produced-at)
                         pulled-at (assoc :pulled-at pulled-at)
                         (some? fencing) (assoc :fencing fencing)
                         ;; The engine merges `:context` into the step's ctx, so
                         ;; this is how the epoch reaches the code that writes.
                         ;; `:fencing` on the envelope alone would not be enough.
                         (some? fencing) (update :context assoc :fencing fencing))]
    (handler-fn e-type e-id env-with-reply)
    (if-not reply-expected?
      {:disposition :ack}
      (let [reply-val (deref reply-p (long (or step-timeout-ms 5000)) ::timeout)]
        (if (= reply-val ::timeout)
          {:disposition :retry
           :error "Timed out waiting for local actor step completion"}
          {:disposition :ack
           :reply reply-val})))))

(defrecord AsyncProxyTransport [grpc-client producer owns-client? options]
  p/ActorTransport
  (send-message! [_ entity-type entity-id envelope opts]
    (let [merged (merge options opts)
          topic (lane-topic (entity-topic (:topic-prefix merged) entity-type)
                            (:topic-suffix merged))
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
          topic (lane-topic (entity-topic (:topic-prefix merged) entity-type)
                            (:topic-suffix merged))
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
          topic (lane-topic (entity-topic (:topic-prefix merged) entity-type)
                            (:topic-suffix merged))
          part (:partition merged)
          parts (:partitions merged)
          ;; Deterministic id, deliberately NOT suffixed with a random uuid.
          ;; This value labels the proxy's async_proxy_queue_depth gauge, so a
          ;; random suffix mints a brand-new Prometheus series on every reconnect
          ;; and abandons the old one -- which keeps reporting its last value for
          ;; the whole staleness window. That is how a fully drained queue still
          ;; summed to ~14k "queued" messages and made the consumers look stuck.
          ;; entity-type + partition is unique per deployment (partitions are
          ;; split across the actor nodes), so it is stable *and* collision-free.
          sub-id (lane-sub-id entity-type part (:topic-suffix merged))
          mode (or (:mode merged) (:delivery-mode merged) :stream)
          step-timeout-ms (or (:step-timeout-ms merged) 5000)
          consumer (ap-cons/start-consumer
                    {:grpc-client grpc-client
                     ;; Pass a logger through so stream disconnects and
                     ;; reconnects are visible; without one the consumer
                     ;; reconnects silently (or, before reconnection existed,
                     ;; did not reconnect at all and said nothing).
                     :logger (:logger merged)
                     :subscriptions
                     {sub-id (cond-> {:subscription-id sub-id
                                      :topic topic
                                      :mode mode
                                      :options (merge {:poll-ms (or (:poll-ms merged) 10)}
                                                      (:subscription-options merged))
                                      :handler #(handle-proxy-delivery handler-fn % step-timeout-ms)}
                               (some? part) (assoc :partition (long part))
                               (some? parts) (assoc :partitions (long parts))
                               ;; Consumer group, when the caller sets one. It
                               ;; scopes the proxy-side durable, so two groups
                               ;; reading the same topic each get every message
                               ;; instead of splitting them. Omitted means the
                               ;; historical ungrouped durable.
                               (:group merged) (assoc :group (:group merged))
                               ;; Ownership credentials, when the deployment
                               ;; enforces ownership. The proxy validates the
                               ;; token at subscribe time and refuses the
                               ;; subscription otherwise, then stamps the
                               ;; resulting fencing epoch on each delivery.
                               (:lease-id merged) (assoc :lease-id (:lease-id merged))
                               (:token merged) (assoc :token (:token merged))
                               ;; Drain control. Passed through so the owner of
                               ;; the lease can wait for a partition to finish
                               ;; its backlog without reaching into the consumer.
                               (:on-drained merged) (assoc :on-drained (:on-drained merged))
                               (:on-pause-blocked merged) (assoc :on-pause-blocked
                                                                 (:on-pause-blocked merged)))}})]
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
