(ns async-worker.rabbitmq.consumer-test
  (:require [async-worker.rabbitmq.consumer :as consumer]
            [async-worker.fixtures :as f]
            [clojure.test :refer :all]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [async-worker.rabbitmq.queue :as queue]
            [langohr.queue :as lq]
            [langohr.channel :as lch]
            [async-worker.rabbitmq.producer :as producer]))

(use-fixtures :each (join-fixtures [f/with-rabbitmq-connection
                                    f/with-rabbitmq-exchange
                                    f/with-bound-queue]))

(deftest consumer-subscriber-count-test
  (testing "starts consumers equal to subscriber-count in args"
    (with-redefs [queue/name (constantly (f/get-queue))]
      (let [subscriber-count 5]
        (consumer/start-subscribers (f/get-connection) "async-test" subscriber-count (fn []))
        (with-open [ch (lch/open (f/get-connection))]
          (is (= subscriber-count (lq/consumer-count ch (f/get-queue)))))))))

(deftest consumer-consume-test
  (testing "converts the message and calls the handler-fn"
    (let [message {:hello :world}
          consumed-messages (atom [])]
      (producer/publish (f/get-connection) (f/get-exchange) "" message true)
      (with-redefs [queue/name (constantly (f/get-queue))]
        (consumer/start-subscribers (f/get-connection) "async-test" 5
                                    (fn [msg] (swap! consumed-messages conj msg))))
      (Thread/sleep 100)
      (is (= [message] @consumed-messages)))))
