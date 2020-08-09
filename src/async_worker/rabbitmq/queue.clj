(ns async-worker.rabbitmq.queue
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [taoensso.nippy :as nippy]
            [camel-snake-kebab.core :as csk]
            [async-worker.utils :as u])
  (:refer-clojure :exclude [name]))

(defn- create-queue [ch queue properties]
  (lq/declare ch queue {:durable true :arguments properties :auto-delete false})
  (log/info "Created queue - " queue))

(defn- bind-queue-to-exchange [ch queue exchange]
  (lq/bind ch queue exchange {:routing-key queue})
  (log/infof "Bound queue %s to exchange %s" queue exchange))

(defn- create-and-bind-queue
  ([connection exchange queue]
   (create-and-bind-queue connection exchange queue {}))
  ([connection exchange queue properties]
   (try
     (with-open [ch (lch/open connection)]
       (create-queue ch queue properties)
       (bind-queue-to-exchange ch queue exchange))
     (catch Exception e
       (throw e)))))

(defn name [namespace queue-type]
  (apply format "%s_%s_queue" (map csk/->snake_case_string [namespace queue-type])))

(defn delay-queue-name [namespace ttl-ms]
  (format "%s_delay_queue_%d_ms" (csk/->snake_case_string namespace) ttl-ms))

(defn- setup-queue [connection namespace exchange queue-type]
  (create-and-bind-queue connection exchange (name namespace queue-type)))

(defn setup-delay-queues [connection namespace exchange retry-max retry-timeout-ms]
  "Setup separate queues for retries - all with dead-letter exchange set to the instant exchange
   Each queue will be used for messages with a speicifc retry-n value"
  (doseq [n (range retry-max)]
    (let [ttl (u/backoff-duration n retry-timeout-ms)]
      (create-and-bind-queue connection
                             exchange
                             (delay-queue-name namespace ttl)
                             {"x-dead-letter-exchange"    exchange
                              "x-dead-letter-routing-key" (name namespace :instant)
                              "x-message-ttl"             ttl}))))

(defn init
  "Initializes the queues required for message processing (instant),
   retries (delay), and dead-set (dead-letter)"
  [connection namespace exchange]
  (setup-queue connection namespace exchange :instant)
  (setup-queue connection namespace exchange :dead-letter))
