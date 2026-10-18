(ns d-core.actor-system.transport-wiring-test
  "What the async-proxy transport must hand to the layers above and below it.

   Both assertions here guard a silent break: a delivery without its epoch looks
   normal, and a subscription without its drain callback looks normal, so neither
   failure surfaces until a handoff goes wrong."
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.transport.async-proxy :as ap-transport]
            [d-core.core.consumers.async-proxy :as ap-cons]
            [d-core.actor-system.behavior :as behavior]
            [d-core.actor-system.runtime.engine :as engine]))

(defn- deliver!
  "Calls the private delivery adapter the way the consumer does."
  [delivery-env handler step-timeout-ms]
  ((var-get #'ap-transport/handle-proxy-delivery) handler delivery-env step-timeout-ms))

(deftest the-fencing-epoch-reaches-the-step
  (let [seen (atom nil)
        handler (fn [_type _id env] (reset! seen env))]
    (is (= {:disposition :ack}
           (deliver! {:payload {:amount 1} :fencing 7} handler 1000)))
    (is (= 7 (:fencing @seen))
        "a write can only fence itself if the step sees the epoch")
    (is (= 7 (get-in @seen [:context :fencing]))
        "and it must be in :context, which is the map the engine merges into the step ctx")))

(deftest no-epoch-means-no-key
  (testing "absence must not turn into a zero that looks like a real epoch"
    (let [seen (atom nil)
          handler (fn [_type _id env] (reset! seen env))]
      (deliver! {:payload {:amount 1}} handler 1000)
      (is (not (contains? @seen :fencing))))))

(deftest subscriptions-carry-ownership-and-drain-wiring
  (let [captured (atom nil)
        on-drained (fn [_] :drained)
        transport (ap-transport/create-async-proxy-transport
                   {:grpc-client :stub-client
                    :partitions 4
                    :group "actors"
                    :lease-id "actors/p0"
                    :token "token-1"
                    :on-drained on-drained})]
    (with-redefs [ap-cons/start-consumer (fn [opts]
                                           (reset! captured opts)
                                           {:handles [] :grpc-client :stub-client})]
      (let [sub (p/subscribe-entities! transport :precinct (fn [_ _ _]) {:partition 0})
            spec (get-in @captured [:subscriptions (:subscription-id sub)])]
        (testing "ownership credentials and group reach the proxy"
          (is (= "actors" (:group spec)))
          (is (= "actors/p0" (:lease-id spec)))
          (is (= "token-1" (:token spec))))
        (testing "partition routing is unchanged"
          (is (= 0 (:partition spec)))
          (is (= 4 (:partitions spec))))
        (testing "the drain callback is forwarded, so a handoff can wait on it"
          (is (identical? on-drained (:on-drained spec))))))))

(deftest peek-state-reads-without-activating
  ;; The read lane answers from memory; peek-state must never create an actor or
  ;; load a snapshot as a side effect of a read.
  (let [spec {:init-fn (fn [_] {:hits 0})
              :step-fn (fn [s _ _] [s {:ok true} []])}
        engine (engine/create-actor-engine {:thing spec})]
    (is (nil? (engine/peek-state engine :thing "t1"))
        "an inactive entity is not peeked into existence")
    (engine/dispatch-envelope! engine :thing "t1" {:type :message :payload {:type :noop}})
    (is (some? (engine/peek-state engine :thing "t1"))
        "an active entity exposes its committed state")))
