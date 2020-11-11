(ns async-worker.rabbitmq.consumer
  (:require [async-worker.rabbitmq.payload :as payload]
            [async-worker.rabbitmq.queue :as queue]
            [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.consumers :as lcons]))

(defn- ack-message
  [ch delivery-tag]
  (lb/ack ch delivery-tag))

(defn process-message-from-queue [ch meta payload handler-fn]
  (let [delivery-tag (:delivery-tag meta)
        message      (payload/decode payload)]
    (when message
      (log/infof "Processing message [%s] from RabbitMQ " message)
      (try
        (log/debug "Calling handler-fn with the message - " message " with retry count - " (:retry-count message))
        (handler-fn message)
        (ack-message ch delivery-tag)
        (catch Exception e
          (log/error e "Error while processing message from RabbitMQ")
          (lb/reject ch delivery-tag true)
          (throw e))))))

(defn- message-handler [handler-fn]
  (fn [ch meta ^bytes payload]
    (process-message-from-queue ch meta payload handler-fn)))

(defn- start-subscriber*
  "Returns consumer-tag"
  [ch prefetch-count queue-name handler-fn]
  (lb/qos ch prefetch-count)
  (lcons/subscribe ch
                   queue-name
                   (message-handler handler-fn)
                   {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                 (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                    :handle-consume-ok-fn      (fn [consumer_tag]
                                                 (log/infof "consumer started for %s with consumer tag %s " queue-name consumer_tag))}))

(def prefetch-count 1)

(defn start-subscribers
  "Starts the subscriber to the instant queue of the rabbitmq
   config expects:
   :rabbitmq - config specific to rabbitmq,including subscriber-count
   :namespace
   :connection - a rabbitmq connection object"
  [connection namespace subscriber-count handler-fn]
  (dotimes [_ subscriber-count]
    (start-subscriber* (lch/open connection)
                       prefetch-count
                       (queue/name namespace :instant)
                       handler-fn)))
