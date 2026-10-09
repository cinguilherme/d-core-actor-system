(ns d-core.actor-system.runtime-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.runtime.actor :as actor]
            [d-core.actor-system.storage.in-memory :as in-mem-storage]))

(def bank-account-spec
  {:init-fn (fn [id] {:id id :balance 0})
   :step-fn (fn [state msg _ctx]
              (case (:op msg)
                :deposit
                (let [new-bal (+ (:balance state) (:amount msg))]
                  [(assoc state :balance new-bal) {:ok true :balance new-bal}])

                :withdraw
                (if (>= (:balance state) (:amount msg))
                  (let [new-bal (- (:balance state) (:amount msg))]
                    [(assoc state :balance new-bal) {:ok true :balance new-bal}])
                  [state {:error "Insufficient funds"}])

                [state {:error "Unknown op"}]))
   :snapshot-every-n 2})

(deftest actor-lifecycle-and-ask-test
  (testing "creates actor, processes messages sequentially and replies"
    (let [store (in-mem-storage/create-in-memory-snapshot-store)
          act (actor/create-actor :account "acc-1" bank-account-spec store {:id "acc-1" :balance 100})]
      (is (actor/active? act))
      (is (= 100 (:balance (actor/get-state act))))

      ;; First deposit
      (let [reply-p (promise)]
        (actor/send-envelope! act {:type :message
                                   :payload {:op :deposit :amount 50}
                                   :reply-fn #(deliver reply-p %)})
        (let [reply (deref reply-p 2000 :timeout)]
          (is (= {:ok true :balance 150} reply))
          (is (= 150 (:balance (actor/get-state act))))))

      ;; Second deposit (triggers snapshot since snapshot-every-n is 2)
      (let [reply-p (promise)]
        (actor/send-envelope! act {:type :message
                                   :payload {:op :deposit :amount 50}
                                   :reply-fn #(deliver reply-p %)})
        (let [reply (deref reply-p 2000 :timeout)]
          (is (= {:ok true :balance 200} reply))
          (is (= 200 (:balance (actor/get-state act))))))

      ;; Verify snapshot was taken
      (let [snap (p/load-snapshot store :account "acc-1" {})]
        (is (= {:id "acc-1" :balance 200} snap)))

      ;; Stop actor
      (let [stop-res (actor/stop-actor! act 2000)]
        (is (= :stopped (:status stop-res)))
        (is (false? (actor/active? act)))))))

(deftest actor-concurrency-serialization-test
  (testing "100 concurrent messages are serialized safely with no lost updates"
    (let [store (in-mem-storage/create-in-memory-snapshot-store)
          act (actor/create-actor :account "acc-concurrency" bank-account-spec store {:id "acc-concurrency" :balance 0})
          n 100
          promises (mapv (fn [_] (promise)) (range n))]
      (doseq [p promises]
        (actor/send-envelope! act {:type :message
                                   :payload {:op :deposit :amount 1}
                                   :reply-fn #(deliver p %)}))

      ;; Await all promises
      (doseq [p promises]
        (is (= true (:ok (deref p 5000 :timeout)))))

      (is (= 100 (:balance (actor/get-state act))))
      (actor/stop-actor! act 2000))))
