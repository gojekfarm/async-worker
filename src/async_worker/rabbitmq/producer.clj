(ns async-worker.rabbitmq.producer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [taoensso.nippy :as nippy]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.exchange :as exchange]
            [async-worker.utils :as u]))

(defn- properties-for-publish
  [expiration]
  (let [props {:content-type "application/octet-stream"
               :persistent   true
               :headers      {}}]
    (if expiration
      (assoc props :expiration (str expiration))
      props)))

(defn- publish
  ([connection exchange routing-key message-payload]
   (publish connection exchange routing-key message-payload nil))
  ([connection exchange routing-key message-payload expiration]
   (try
     (u/with-retry {:count 5 :wait 100}
       (with-open [ch (lch/open connection)]
         (lb/publish ch
                     exchange
                     routing-key
                     (nippy/freeze message-payload)
                     (properties-for-publish expiration))))
     (catch Throwable e
       (log/error "Publishing message to rabbitmq failed with error " (.getMessage e))
       (throw e)
       #_(sentry/report-error sentry-reporter e
                            "Pushing message to rabbitmq failed, data: " message-payload)))))

(defn enqueue [connection namespace message]
  (publish connection
           (exchange/name namespace)
           (queue/name namespace :instant)
           message))

(defn move-to-dead-set [connection namespace message]
  (publish connection
           (exchange/name namespace)
           (queue/name namespace :dead-letter)
           message))

(defn requeue [connection namespace message current-iteration retry-timeout-ms]
  (publish connection
           (exchange/name namespace)
           (queue/delay-queue-name namespace (u/backoff-duration current-iteration retry-timeout-ms))
           message))
