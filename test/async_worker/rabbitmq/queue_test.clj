(ns async-worker.rabbitmq.queue-test
  (:require [async-worker.rabbitmq.queue :as queue]
            [async-worker.fixtures :as f]
            [clojure.test :refer :all]
            [async-worker.utils :as u]
            [langohr.channel :as lch]
            [langohr.queue :as lq]))

(use-fixtures :each (compose-fixtures f/with-rabbitmq-connection f/with-rabbitmq-exchange))

(defn- queue-exists? [queue-name]
  (try
    ;; throws exception if queue doesn't exist
    (with-open [ch (lch/open (f/get-connection))]
      (lq/declare-passive ch queue-name))
       true
       (catch Exception e
         false)))

(defn delete-queue [q]
  (with-open [ch (lch/open (f/get-connection))]
    (lq/delete ch q)))

(deftest init-test
  (testing "queue init creates the required instant and dead-letter queues"
    (let [namespace (str (gensym "async-test-ns-"))]
      (queue/init (f/get-connection) namespace (f/get-exchange))
      (queue-exists? (queue/name namespace :instant))
      (queue-exists? (queue/name namespace :dead-letter))

      (delete-queue (queue/name namespace :instant))
      (delete-queue (queue/name namespace :dead-letter)))))

(deftest setup-delay-queues-test
  (testing "creates the required delay queues"
    (let [namespace       (str (gensym "async-test-ns-"))
          retry-ms        100
          retry-max       3
          expected-delays (map #(u/backoff-duration % retry-ms) (range retry-max))
          expected-queues (map #(queue/delay-queue-name namespace %) expected-delays)]
      (queue/setup-delay-queues (f/get-connection) namespace (f/get-exchange) retry-max retry-ms)
      (is (every? true? (map queue-exists? expected-queues)))

      (dorun (map delete-queue expected-queues)))))
