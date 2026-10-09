(ns d-core.actor-system.end-to-end-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.core :as core]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.system]
            [integrant.core :as ig]))

;; Realistic Auction Entity with strict contention safety
(def auction-spec
  {:init-fn (fn [auction-id]
              {:id auction-id
               :status :open
               :highest-bid 0
               :highest-bidder nil
               :bid-history []})
   :step-fn (fn [state msg _ctx]
              (case (:op msg)
                :bid
                (let [{:keys [bidder amount]} msg]
                  (if (and (= :open (:status state))
                           (> amount (:highest-bid state)))
                    (let [new-state (-> state
                                        (assoc :highest-bid amount
                                               :highest-bidder bidder)
                                        (update :bid-history conj {:bidder bidder :amount amount}))]
                      [new-state {:ok true :highest-bid amount :bidder bidder}])
                    [state {:ok false :error "Bid too low or auction closed"}]))

                :close
                (let [new-state (assoc state :status :closed)]
                  [new-state {:ok true :status :closed :winning-bid (:highest-bid state)}])

                :status
                [state {:ok true :state state}]

                [state {:ok false :error "Unknown op"}]))
   :idle-timeout-ms 500
   :snapshot-every-n 2})

(deftest end-to-end-integrant-system-test
  (testing "full system lifecycle with Integrant, concurrent bids and contention protection"
    (let [system-config
          {:d-core.actor-system/storage
           {:type :in-memory}

           :d-core.actor-system/transport
           {:type :in-memory}

           :d-core.actor-system/client
           {:transport (ig/ref :d-core.actor-system/transport)}

           :d-core.actor-system/runtime
           {:transport (ig/ref :d-core.actor-system/transport)
            :storage (ig/ref :d-core.actor-system/storage)
            :entities {:auction auction-spec}
            :options {:watchdog-interval-ms 100
                      :default-idle-timeout-ms 500}}}

          sys (ig/init system-config)
          client (:d-core.actor-system/client sys)
          runtime (:d-core.actor-system/runtime sys)
          auction-id "art-auction-99"]

      (try
        ;; 1. Initial bid
        (let [res (core/ask-sync! client :auction auction-id {:op :bid :bidder "Alice" :amount 100})]
          (is (= {:ok true :highest-bid 100 :bidder "Alice"} res)))

        ;; 2. Concurrent bids from 20 bidders attempting amounts 101 to 120 in parallel
        (let [bids (mapv (fn [i] {:bidder (str "Bidder-" i) :amount (+ 100 i)}) (range 1 21))
              futures (mapv (fn [b]
                              (core/ask! client :auction auction-id (merge {:op :bid} b)))
                            (shuffle bids))
              results (mapv deref futures)
              successful-bids (filter :ok results)]
          ;; Every bid should either succeed or fail deterministically without corrupting state
          (is (= 20 (count results)))
          (let [status-res (core/ask-sync! client :auction auction-id {:op :status})]
            (is (= 120 (get-in status-res [:state :highest-bid])))
            ;; 1 initial Alice bid + number of successful concurrent bids
            (is (= (+ 1 (count successful-bids))
                   (count (get-in status-res [:state :bid-history]))))))

        ;; 3. Close the auction
        (let [close-res (core/ask-sync! client :auction auction-id {:op :close})]
          (is (= {:ok true :status :closed :winning-bid 120} close-res)))

        ;; 4. Check active actors before passivation
        (let [active (p/active-actors (:engine runtime) {})]
          (is (= 1 (count active))))

        ;; 5. Wait for idle passivation (timeout is 500ms, watchdog runs every 100ms)
        (Thread/sleep 650)

        ;; Active actors should now be 0 (evicted from memory)
        (let [active-after (p/active-actors (:engine runtime) {})]
          (is (= 0 (count active-after))))

        ;; 6. Query status again -> rehydrates from storage snapshot
        (let [rehydrated-status (core/ask-sync! client :auction auction-id {:op :status})]
          (is (= :closed (get-in rehydrated-status [:state :status])))
          (is (= 120 (get-in rehydrated-status [:state :highest-bid]))))

        (finally
          (ig/halt! sys))))))
