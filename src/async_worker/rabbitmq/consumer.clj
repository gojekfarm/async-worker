(ns async-worker.rabbitmq.consumer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.consumers :as lcons]
            [taoensso.nippy :as nippy]
            [async-worker.rabbitmq.queue :as queue]))

(defn convert-message
  "De-serializes the message payload (`payload`) using `nippy/thaw`"
  [^bytes payload]
  (nippy/thaw payload))

(defn- ack-message
  [ch delivery-tag]
  (lb/ack ch delivery-tag))

(defn process-message-from-queue [ch meta payload handler-fn]
  (let [delivery-tag    (:delivery-tag meta)
        message-payload (convert-message payload)]
    (when message-payload
      (log/infof "Processing message [%s] from RabbitMQ " message-payload)
      (try
        (log/debug "Calling handler-fn with the message-payload - " message-payload " with retry count - " (:retry-count message-payload))
        (handler-fn message-payload)
        (ack-message ch delivery-tag)
        (catch Exception e
          (log/error e "Error while processing message-payload from RabbitMQ")
          (lb/reject ch delivery-tag true)
          (throw e)
          #_(sentry/report-error sentry-reporter e "Error while processing message-payload from RabbitMQ")
          #_(metrics/increment-count ["rabbitmq-message" "process"] "failure" {:topic_name (name topic-entity)}))))))

(defn- message-handler [handler-fn]
  (fn [ch meta ^bytes payload]
    (process-message-from-queue ch meta payload handler-fn)))

(defn- start-subscriber*
  "Returns consumer-tag"
  [ch prefetch-count queue-name handler-fn]
  (lb/qos ch prefetch-count)
  (lcons/subscribe ch                                       ;; new thread
                   queue-name
                   (message-handler handler-fn)
                   {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                 (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                    :handle-consume-ok-fn      (fn [consumer_tag]
                                                 (log/infof "consumer started for %s with consumer tag %s " queue-name consumer_tag))}))

(defn start-subscribers
  "Starts the subscriber to the instant queue of the rabbitmq
   config expects:
   :worker-count - The number of subscribers to initialize
   :prefetch-count
   :queue-name - as defined for the producer, internal queue-names will be prefixed with this supplied name"
  [connection handler-fn {:keys [worker-count prefetch-count queue-name] :as config}]
  (dotimes [_ (:worker-count config)]
    (start-subscriber* (lch/open connection)
                       prefetch-count
                       (queue/queue queue-name ::instant)
                       handler-fn)))
