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
   (create-and-bind-queue connection queue exchange nil))
  ([connection queue exchange properties]
   (let [;properties (if dead-letter-exchange {"x-dead-letter-exchange" dead-letter-exchange
         ;                                     "x-dead-letter-routing-key" dead-letter-exchange-routing-key
         ;                                     "x-message-ttl" }
         ;               {})
         ch (atom nil)]
     (try
       (reset! ch (lch/open connection))
       (create-queue @ch queue properties)
       (bind-queue-to-exchange @ch queue exchange)
       (catch Exception e (throw e))
       (finally (lch/close @ch))))))

(defn exp-back-off [base-expiration-time retry-n] (Math/pow 2 retry-n)
  (-> (Math/pow 2 retry-n)
      (* base-expiration-time)
      long))

(defn next-exp-back-off [current-expiration-time base-expiration-time retry-n max-retry-n])

(defn queue [namespace queue-type]
  (apply format "%s_%s_queue" (map csk/->snake_case_string [namespace queue-type])))

(defn delay-queue [queue-name expiration-time]
  (format "%s_delay_queue_%d" (csk/->snake_case_string queue-name) expiration-time))

(defn- setup-queue [connection namespace queue-type exchange]
  (create-and-bind-queue connection (queue namespace (name queue-type)) exchange))

(defn- setup-delay-queues [connection namespace base-expiration-time max-retry-count exchange]
  "Setup separate queues for retries - all with dead-letter exchange set to the instant exchange
   Each queue will be used for messages with a speicifc retry-n value"
  (doseq [n (range max-retry-count)]
    (let [expiration-time (exp-back-off base-expiration-time n)]
      (create-and-bind-queue connection
                             (delay-queue namespace expiration-time)
                             exchange
                             {"x-dead-letter-exchange"    exchange
                              "x-dead-letter-routing-key" (queue namespace :instant)
                              "x-message-ttl"             (* 1000 expiration-time)}))))

(defn create-and-bind-queues
  "Initializes the queues and exchanges required for message processing (instant),
   retries (delay), and dead-set (dead-letter)"
  [connection {:keys [namespace base-expiration-time-ms max-retry-count exchange] :as config}]
  (prn config)
  (setup-queue connection namespace :instant exchange)
  (setup-queue connection namespace :dead-letter exchange)
  ; also make linear backoff queues?
  (setup-delay-queues connection namespace base-expiration-time-ms max-retry-count exchange))
