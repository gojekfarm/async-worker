(ns async-worker.rabbitmq.queue
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [taoensso.nippy :as nippy]
            [camel-snake-kebab.core :as csk]
            [async-worker.rabbitmq.exchange :as e]))

(defn- create-queue [ch queue props]
  (lq/declare ch queue {:durable true :arguments props :auto-delete false})
  (log/info "Created queue - " queue))

(defn- bind-queue-to-exchange [ch queue-name exchange-name]
  (lq/bind ch queue-name exchange-name {:routing-key queue-name})
  (log/infof "Bound queue %s to exchange %s with routing key %s" queue-name exchange-name queue-name))

(defn- create-and-bind-queue
  ([connection queue exchange]
   (create-and-bind-queue connection queue exchange nil nil))
  ([connection queue exchange dead-letter-exchange dead-letter-exchange-routing-key]
   (let [properties (if dead-letter-exchange {"x-dead-letter-exchange" dead-letter-exchange
                                              "x-dead-letter-routing-key" dead-letter-exchange-routing-key}
                        {})
         ch (atom nil)]
     (try
       (reset! ch (lch/open connection))
       (create-queue @ch queue properties)
       (bind-queue-to-exchange @ch queue exchange)
       (catch Exception e (throw e))
       (finally (lch/close @ch))))))

(defn- exp-back-off [base-expiration-time retry-n] (Math/pow 2 retry-n)
  (-> (Math/pow 2 retry-n)
      (* base-expiration-time)
      long))

(defn queue [namespace queue-type]
  (apply format "%s_%s_queue" (map csk/->snake_case_string [namespace queue-type])))

(defn delay-queue [queue-name base-expiration-time retry-n]
  (format "%s_delay_queue_%d" (csk/->snake_case_string queue-name) (exp-back-off base-expiration-time retry-n)))

(defn- setup-queue [connection namespace queue-type]
  (let [queue-type (name queue-type)]
    (create-and-bind-queue connection (queue namespace queue-type) (e/exchange namespace))))

(defn- setup-delay-queues [connection namespace base-expiration-time max-retry-count]
  "Setup separate queues for retries - all with dead-letter exchange set to the instant exchange
   Each queue will be used for messages with a speicifc retry-n value"
  (let [exchange-name (e/exchange namespace)]
    (doseq [n (range max-retry-count)]
      (create-and-bind-queue connection
                             (delay-queue namespace base-expiration-time n)
                             exchange-name exchange-name
                             (queue namespace :instant)))))

(defn make-queues
  "Initializes the queues and exchanges required for message processing (instant),
   retries (delay), and dead-set (dead-letter)"
  [connection {:keys [namespace base-expiration-time-ms max-retry-count] :as config}]
  (setup-queue connection namespace :instant)
  (setup-queue connection namespace :dead-letter)
  ; also make linear backoff queues?
  (setup-delay-queues connection namespace base-expiration-time-ms max-retry-count))
