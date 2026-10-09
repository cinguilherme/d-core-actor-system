(ns d-core.actor-system.engine-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.runtime.engine :as engine]
            [d-core.actor-system.storage.in-memory :as in-mem-storage]))

(def test-item-spec
  {:init-fn (fn [id] {:id id :stock 100})
   :step-fn (fn [state msg _ctx]
              (case (:op msg)
                :reserve
                (if (>= (:stock state) (:qty msg))
                  (let [new-stock (- (:stock state) (:qty msg))]
                    [(assoc state :stock new-stock) {:ok true :remaining new-stock}])
                  [state {:error "Out of stock"}])
                :restock
                (let [new-stock (+ (:stock state) (:qty msg))]
                  [(assoc state :stock new-stock) {:ok true :remaining new-stock}])
                [state {:error "unknown"}]))
   :idle-timeout-ms 200      ;; 200ms idle timeout for quick test
   :snapshot-every-n 1})

(deftest engine-activation-and-passivation-test
  (testing "engine activates on dispatch, runs, saves snapshot, and passivates when idle"
    (let [store (in-mem-storage/create-in-memory-snapshot-store)
          engine (engine/create-actor-engine {:item test-item-spec}
                                             store
                                             {:watchdog-interval-ms 50
                                              :default-idle-timeout-ms 200})]
      (try
        ;; Reserve 10 items
        (let [reply-p (promise)]
          (engine/dispatch-envelope! engine :item "item-1"
                                     {:type :message
                                      :payload {:op :reserve :qty 10}
                                      :reply-fn #(deliver reply-p %)})
          (is (= {:ok true :remaining 90} (deref reply-p 2000 :timeout))))

        ;; Check in-memory state directly
        (is (= 90 (:stock (p/get-actor-state engine :item "item-1" {}))))
        (is (= 1 (count (p/active-actors engine {}))))

        ;; Wait for idle timeout (watchdog runs every 50ms, idle timeout is 200ms)
        (Thread/sleep 350)

        ;; Should have passivated
        (is (= 0 (count (p/active-actors engine {}))))
        (is (nil? (p/get-actor-state engine :item "item-1" {})))

        ;; Snapshot must be persisted in store
        (let [saved (p/load-snapshot store :item "item-1" {})]
          (is (= {:id "item-1" :stock 90} saved)))

        ;; Send another message - actor should rehydrate with stock 90!
        (let [reply-p (promise)]
          (engine/dispatch-envelope! engine :item "item-1"
                                     {:type :message
                                      :payload {:op :reserve :qty 20}
                                      :reply-fn #(deliver reply-p %)})
          (is (= {:ok true :remaining 70} (deref reply-p 2000 :timeout))))

        (is (= 70 (:stock (p/get-actor-state engine :item "item-1" {}))))

        (finally
          (engine/stop-actor-engine! engine))))))
