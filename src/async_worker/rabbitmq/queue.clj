(ns async-worker.rabbitmq.queue
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [taoensso.nippy :as nippy]
            [camel-snake-kebab.core :as csk]))

(defn- create-queue [queue props ch]
  (lq/declare ch queue {:durable true :arguments props :auto-delete false})
  (log/info "Created queue - " queue))

(defn- declare-exchange [ch exchange]
  (le/declare ch exchange "fanout" {:durable true :auto-delete false})
  (log/info "Declared exchange - " exchange))

(defn- bind-queue-to-exchange [ch queue exchange]
  (lq/bind ch queue exchange)
  (log/infof "Bound queue %s to exchange %s" queue exchange))

(defn- create-and-bind-queue
  ([connection queue-name exchange]
   (create-and-bind-queue connection queue-name exchange nil))
  ([connection queue-name exchange-name dead-letter-exchange]
   (try
     (let [props (if dead-letter-exchange
                   {"x-dead-letter-exchange" dead-letter-exchange}
                   {})]
       (let [ch (lch/open connection)]
         (create-queue queue-name props ch)
         (declare-exchange ch exchange-name)
         (bind-queue-to-exchange ch queue-name exchange-name)))
     (catch Exception e
       #_(sentry/report-error sentry-reporter e "Error while declaring RabbitMQ queues")
       (throw e)))))

(defn queue [queue-name queue-type]
  (apply format "%s_%s_queue" (map csk/->snake_case_string [queue-name queue-type])))

(defn exchange [queue-name queue-type]
  (apply format "%s_%s_exchange" (map csk/->snake_case_string [queue-name queue-type])))

(defn delay-queue [queue-name retry-n]
  (format "%s_delay_queue_%d" (csk/->snake_case_string queue-name) retry-n))

(defn delay-exchange [queue-name retry-n]
  (format "%s_delay_exchage_%d" (csk/->snake_case_string queue-name) retry-n))

(defn- setup-queue [connection queue-name queue-type]
  (let [queue-type (name queue-type)]
    (create-and-bind-queue connection (queue queue-name queue-type) (exchange queue-name queue-type))))

(defn- setup-delay-queues [connection queue-name retry-count]
  (doseq [n (range retry-count)]
    (create-and-bind-queue connection
                           (delay-queue queue-name n)
                           (delay-exchange queue-name n)
                           (exchange queue-name :instant)))) ;; add dlx

(defn make-queues
  "Initializes the queues and exchanges required for message processing (instant),
   retries (delay), and dead-set (dead-letter)"
  [connection {:keys [queue-name retry-count] :as config}]
  (setup-queue connection queue-name :instant)
  (setup-queue connection queue-name :dead-letter)
  ; also make linear backoff queues?
  (setup-delay-queues connection queue-name retry-count))
