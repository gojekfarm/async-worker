(ns async-worker.rabbitmq.producer-test
  (:require [async-worker.rabbitmq.producer :as producer]
            [async-worker.rabbitmq.channel :as channel]
            [async-worker.fixtures :as f]
            [clojure.test :refer :all]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]))

(use-fixtures :each (join-fixtures [f/with-rabbitmq-connection
                                    f/with-channel-pool
                                    f/with-rabbitmq-exchange
                                    f/with-bound-queue]))

(deftest publish-test
  (testing "publish succeeds"
    (let [message {:hello :world}]
      (producer/publish (f/get-channel-pool)
                        (f/get-exchange)
                        ""
                        message true)))

  (testing "publish freezes message"
    (let [message {:hello :world}
          args (atom {})]
      (with-redefs [lb/publish (fn [ch exchange routing-key payload properties]
                                 (reset! args {:payload payload :properties properties}))]
        (producer/publish (f/get-channel-pool)
                          (f/get-exchange)
                          ""
                          message
                          true)

        (is (= message (nippy/thaw (:payload @args)))))))

  (testing "returned messages are picked up by the listener"
    (let [message {:hello :world}
          returned-arg (atom nil)
          return-handler (fn [arg] (reset! returned-arg arg))
          channel-pool (channel/create-pool (f/get-connection) 5 {:return-listener return-handler})]
      (producer/publish channel-pool
                        (f/get-exchange)
                        "wrong-routing-key"
                        message
                        true)
      (is (some? @returned-arg))
      (is (= message (:body @returned-arg))))))
