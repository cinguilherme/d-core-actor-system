(ns d-core.actor-system.storage-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.actor-system.protocols :as p]
            [d-core.actor-system.storage.store-adapter :as adapter]
            [d-core.core.storage.protocol :as sp]))

(defrecord MockStorage [store-atom]
  sp/StorageProtocol
  (storage-get [_ key _opts]
    (get @store-atom key))
  (storage-put [_ key value _opts]
    (swap! store-atom assoc key value)
    value)
  (storage-delete [_ key _opts]
    (swap! store-atom dissoc key))
  (storage-get-bytes [_ _ _] nil)
  (storage-put-bytes [_ _ _ _] nil)
  (storage-head [_ _ _] nil)
  (storage-list [_ _] nil))

(deftest store-adapter-test
  (testing "saves and loads snapshots via StorageProtocol"
    (let [mock-db (->MockStorage (atom {}))
          snap-store (adapter/create-storage-adapter mock-db {:prefix "test-actors"})]

      ;; Initial load returns nil
      (is (nil? (p/load-snapshot snap-store :user "u1" {})))

      ;; Save snapshot
      (p/save-snapshot! snap-store :user "u1" {:name "Bob" :age 30} {})

      ;; Verify raw storage contains json
      (is (contains? @(:store-atom mock-db) "test-actors/user/u1.json"))

      ;; Load snapshot parses back to Clojure map with keyword keys
      (let [loaded (p/load-snapshot snap-store :user "u1" {})]
        (is (= {:name "Bob" :age 30} loaded))))))
