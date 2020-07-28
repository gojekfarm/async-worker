(ns async-worker.rabbitmq.producer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [taoensso.nippy :as nippy]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.retry :refer [with-retry]]))

(defn- properties-for-publish
  [expiration]
  (let [props {:content-type "application/octet-stream"
               :persistent   true
               :headers      {}}]
    (if expiration
      (assoc props :expiration (str expiration))
      props)))

(defn- publish
  ([connection exchange message-payload]
   (publish connection exchange message-payload nil))
  ([connection exchange message-payload expiration]
   (try
     (with-retry {:count 5 :wait 100}
       (with-open [ch (lch/open connection)]
         (lb/publish ch
                     exchange
                     ""
                     (nippy/freeze message-payload)
                     (properties-for-publish expiration))))
     (catch Throwable e
       (log/error "Publishing message to rabbitmq failed with error " (.getMessage e))
       (throw e)
       #_(sentry/report-error sentry-reporter e
                            "Pushing message to rabbitmq failed, data: " message-payload)))))

(defn enqueue [connection queue-name message]
  (publish connection (queue/exchange queue-name :instant) message))

(defn move-to-dead-set [connection queue-name message]
  (publish connection (queue/exchange queue-name :dead-letter) message))

(defn- backoff-duration [retry-n timeout-ms]
  (-> (Math/pow 2 retry-n)
      (* timeout-ms)
      int))

(defn requeue [connection queue-name message retry-n retry-timeout-ms]
  (publish connection
           (queue/delay-exchange queue-name retry-n)
           message
           (backoff-duration retry-n retry-timeout-ms)))
