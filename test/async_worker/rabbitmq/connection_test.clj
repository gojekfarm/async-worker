(ns async-worker.rabbitmq.connection-test
  (:require [async-worker.rabbitmq.connection :as connection]
            [clojure.test :refer :all]
            [async-worker.fixtures :as f])
  (:import [com.novemberain.langohr Connection]))

(deftest connection-test
  (testing "starts and stops connection normally without external executor"
    (let [conn (connection/start-connection f/connection-config)]
      (is (instance? Connection conn))
      (is (true? (.isOpen conn)))
      (connection/stop-connection conn)))

  (testing "starts and stops connection normally with external executor"
    (let [executor (java.util.concurrent.Executors/newFixedThreadPool 5)
          conn (connection/start-connection (assoc f/connection-config :executor executor))]
      (is (instance? Connection conn))
      (is (true? (.isOpen conn)))
      (connection/stop-connection conn)))

  (testing "throws exception if unable to start a connection"
    (is (thrown? Exception (connection/start-connection (assoc f/connection-config :hosts ["404"]))))))
