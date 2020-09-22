(ns async-worker.rabbitmq.producer-test
  (:require [async-worker.rabbitmq.producer :as producer]
            [async-worker.fixtures :as f]
            [clojure.test :refer :all]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]))

(use-fixtures :each (join-fixtures [f/with-rabbitmq-connection
                                    f/with-rabbitmq-exchange
                                    f/with-bound-queue]))

(deftest publish-test
  (testing "publish succeeds"
    (let [message {:hello :world}]
      (producer/publish (f/get-connection)
                        (f/get-exchange)
                        ""
                        message true)))

  (testing "publish freezes message"
    (let [message {:hello :world}
          args (atom {})]
      (with-redefs [lb/publish (fn [ch exchange routing-key payload properties]
                                 (reset! args {:payload payload :properties properties}))]
        (producer/publish (f/get-connection)
                          (f/get-exchange)
                          ""
                          message
                          true)

        (is (= message (nippy/thaw (:payload @args)))))))

  (testing "publish throws exception for unroutable message"
    (let [message {:hello :world}]
      (is (thrown? Exception
                   (producer/publish (f/get-connection)
                                     (f/get-exchange)
                                     "wrong-routing-key"
                                     message
                                     true))))))
