(ns async-worker.rabbitmq.consumer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.consumers :as lcons]
            [taoensso.nippy :as nippy]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.exchange :as exc]
            [async-worker.rabbitmq.producer :as producer]))

(defn convert-message
  "De-serializes the message payload (`payload`) using `nippy/thaw`"
  [^bytes payload]
  (nippy/thaw payload))

(defn- ack-message
  [ch delivery-tag]
  (lb/ack ch delivery-tag))


;;;
;(comment
;  (let [{retry? ::retry? retry-n ::retry-n :as job-params}]
;    (try
;      (execute job-params)
;      (catch Exception e
;        (if (and retry? (< retry-n retry-count))
;          (producer/enqueue connection
;                            (exc/exchange namespace)
;                            "trips_delay_queue_100"         ;; constant delay queue
;                            {:my-message 10 ::retry-n 1 ::retry true}
;                            ;(update job-params ::retry-n inc)
;                            ;retry-n
;                            ;retry-timeout-ms
;                            ))))))
;

(defn execute-with-retry [conn message-payload jobs]
  (let [meta-keys [:retry-n :max-retry-count :base-expiration-time :namespace :handler]
        meta-info (select-keys message-payload meta-keys)
        payload (:message (apply dissoc message-payload meta-keys))
        {:keys [retry-n max-retry-count base-expiration-time namespace handler]} meta-info
        handler-fn (:handler (get jobs handler))]
    (try
      (handler-fn payload)
      (catch Exception ex                                   ;; TO DO
        (let [exchange (exc/exchange namespace)
              retry-new-n (inc retry-n)
              expiration-time (queue/exp-back-off base-expiration-time retry-new-n)
              queue-as-routing-key (if (< retry-n max-retry-count)
                                     (queue/delay-queue namespace expiration-time)
                                     (queue/queue namespace :dead-letter))
              payload-new (assoc meta-info :retry-n retry-new-n :message payload)]
          (producer/enqueue conn exchange queue-as-routing-key payload-new))
        (throw ex)))))

(defn process-message-from-queue [ch meta payload jobs]
  (let [delivery-tag    (:delivery-tag meta)
        message-payload (convert-message payload)]
    (when message-payload
      (log/infof "Processing message [%s] from RabbitMQ " message-payload)
      (try
        (log/debug "Calling handler-fn with the message-payload - " message-payload " with retry count - " (:retry-count message-payload))
        (execute-with-retry ch message-payload jobs)
        ;; the user only provide the execute method, so the execute-with-retry is implemented here
        ;(comment
        ;  (let [handler-fn-key (:handler message-payload)   ;; already a keyword
        ;        handler-fn (get message-payload handler-fn-key (fn default-handler [x y z] "default-handler"))]
        ;
        ;
        ;    (execute-with-retry ch message-payload handler-fn))
        ;
        ;  )

        (ack-message ch delivery-tag)
        (catch Exception e
          (log/error e "Error while processing message-payload from RabbitMQ")
          (lb/reject ch delivery-tag true)
          (throw e)
          #_(sentry/report-error sentry-reporter e "Error while processing message-payload from RabbitMQ")
          #_(metrics/increment-count ["rabbitmq-message" "process"] "failure" {:topic_name (name topic-entity)}))))))

(defn- message-handler [handler-fn-map]
  (fn [ch meta ^bytes payload]
    (process-message-from-queue ch meta payload handler-fn-map)))

(defn- start-subscriber*
  "Returns consumer-tag"
  [ch prefetch-count queue-name handler-fn-map]
  (lb/qos ch prefetch-count)
  (lcons/subscribe ch                                       ;; new thread
                   queue-name
                   (message-handler handler-fn-map)
                   {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                 (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                    :handle-consume-ok-fn      (fn [consumer_tag]
                                                 (log/infof "consumer started for %s with consumer tag %s " queue-name consumer_tag))}))

;(defn my-handler-fn-example [payload]
;  (prn payload))
;
;(defn handler-fn-example [handler-fn]
;  (let [{:keys [retry-n max-retry-count base-expiration-time payload]} (convert-message message)]
;    (prn (str retry-n max-retry-count base-expiration-time payload))))

;{:namespace  "trips"
; :connection {:host               "localhost"
;              :port               5672
;              :username           "guest"
;              :password           "guest"
;              :connection-timeout 2000}
; :worker     {:count          5
;              :executor       executor
;              :prefetch-count 1}
; :jobs       {:auto-arrival {:retry-count      5
;                             :retry-timeout-ms 100
;                             :handler          handler-fn
;                             }}}


(defn start-subscribers
  "Starts the subscriber to the instant queue of the rabbitmq
   config expects:
   :worker-count - The number of subscribers to initialize
   :prefetch-count
   :queue-name - as defined for the producer, internal queue-names will be prefixed with this supplied name"
  [connection {:keys [namespace worker jobs] :as config}]
  (dotimes [_ (:count worker)]
    (start-subscriber* (lch/open connection)                ; new channel for each worker
                       1                                    ; prefetch count = 1 for consistency
                       (queue/queue namespace :instant)
                       jobs)))
