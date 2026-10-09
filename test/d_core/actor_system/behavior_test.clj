(ns d-core.actor-system.behavior-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.behavior :as behavior]))

(def test-spec
  {:init-fn (fn [id] {:id id :counter 0})
   :step-fn (fn [state msg _ctx]
              (case (:op msg)
                :inc [(update state :counter inc) {:ok true :counter (inc (:counter state))}]
                :dec [(update state :counter dec) {:ok true :counter (dec (:counter state))}]
                :echo [state msg]
                :throw-err (throw (RuntimeException. "Simulated crash"))
                [state {:error "unknown op"}]))
   :snapshot-every-n 5
   :idle-timeout-ms 10000})

(deftest spec-validation-test
  (testing "valid spec succeeds"
    (is (nil? (behavior/validate-entity-spec! test-spec))))

  (testing "invalid spec throws"
    (is (thrown? Exception
          (behavior/validate-entity-spec! {:init-fn "not-a-fn"})))))

(deftest state-initialization-test
  (testing "initializes with init-fn when no snapshot"
    (let [init (behavior/initialize-state test-spec "user-1" nil)]
      (is (= {:id "user-1" :counter 0} init))))

  (testing "initializes with snapshot when present"
    (let [snapshot {:id "user-1" :counter 42}
          init (behavior/initialize-state test-spec "user-1" snapshot)]
      (is (= {:id "user-1" :counter 42} init)))))

(deftest step-execution-test
  (testing "normal step execution with reply"
    (let [state {:id "e-1" :counter 0}
          res (behavior/execute-step test-spec state {:op :inc} {})]
      (is (= {:id "e-1" :counter 1} (:next-state res)))
      (is (= {:ok true :counter 1} (:reply res)))
      (is (empty? (:effects res)))))

  (testing "error during step preserves current state and captures error"
    (let [state {:id "e-1" :counter 10}
          res (behavior/execute-step test-spec state {:op :throw-err} {})]
      (is (= state (:next-state res)))
      (is (= "Simulated crash" (:error res))))))

(deftest should-snapshot-test
  (testing "snapshots on configured intervals"
    (is (false? (behavior/should-snapshot? test-spec 1)))
    (is (false? (behavior/should-snapshot? test-spec 4)))
    (is (true? (behavior/should-snapshot? test-spec 5)))
    (is (true? (behavior/should-snapshot? test-spec 10)))))
