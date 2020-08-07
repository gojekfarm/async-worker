(ns async-worker.rabbitmq.exchange-test
  (:require [async-worker.rabbitmq.exchange :as exchange]
            [async-worker.fixtures :as f]
            [clojure.test :refer :all]
            [langohr.exchange :as le]
            [langohr.channel :as lch]))

(use-fixtures :each f/with-rabbitmq-connection)

(defn- exchange-exists? [exchange-name]
  (try
    ;; throws exception if exchange doesn't exist
    (with-open [ch (lch/open (f/get-connection))]
      (le/declare-passive ch exchange-name))
       true
       (catch Exception e
         false)))

(deftest exchange-test
  (testing "declare creates an exchange"
    (let [exchange-name (str (gensym "async-worker-test-exchange-"))]
      (exchange/declare (f/get-connection) exchange-name)
      (is (true? (exchange-exists? exchange-name))))))
