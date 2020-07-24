(ns async-worker.rabbitmq.producer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [taoensso.nippy :as nippy]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.retry :refer [with-retry]]))

(defn- record-headers->map [record-headers]
  (reduce (fn [header-map record-header]
            (assoc header-map (.key record-header) (String. (.value record-header))))
          {}
          record-headers))

(defn- properties-for-publish
  [expiration headers]
  (let [props {:content-type "application/octet-stream"
               :persistent   true
               :headers      (record-headers->map headers)}]
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
                     (nippy/freeze (dissoc message-payload :headers))
                     (properties-for-publish expiration (:headers message-payload)))))
     (catch Throwable e
       (log/error "Publishing message to rabbitmq failed with error " (.getMessage e))
       (throw e)
       #_(sentry/report-error sentry-reporter e
                            "Pushing message to rabbitmq failed, data: " message-payload)))))

(defn publish-to-instant-queue [connection queue-name message]
  (publish connection (queue/exchange queue-name :instant) message))
