(ns async-worker.rabbitmq.dead-set
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.consumer :as consumer]
            [async-worker.rabbitmq.producer :as producer]))

(defn- dead-set-queue [namespace]
  (queue/name namespace :dead-letter))

(defn- process-dead-set-messages
  [connection queue-name n handler-fn]
  (with-open [ch (lch/open connection)]
    (dotimes [_ n]
      (let [[meta payload] (lb/get ch queue-name false)]
        (when (some? payload)
          (consumer/process-message-from-queue ch meta payload handler-fn))))))

(defn- read-without-ack [ch queue-name]
  (let [[meta payload] (lb/get ch queue-name false)]
    (when (some? payload)
      (lb/reject ch (:delivery-tag meta) true)
      (consumer/convert-message payload))))

(defn- get-dead-set-messages
  [connection queue-name n]
  (->> (with-open [ch (lch/open connection)]
         (doall (for [_ (range n)]
                  (read-without-ack ch queue-name))))
       (remove nil?)))

;; retry replaying messages?
(defn replay
  "Replay messages from dead-set by moving them to the instant queue"
  [connection namespace n]
  (log/debugf "Replaying %d number of messages from dead-letter-queue for %s" n namespace)
  (process-dead-set-messages connection (dead-set-queue namespace) n
                             (fn [payload] (producer/enqueue connection namespace payload))))

(defn delete
  "Deletes messages from dead queue"
  [connection namespace n]
  (log/debugf "Deleting %d number of messages from dead-letter-queue for %s" n namespace)
  (process-dead-set-messages connection (dead-set-queue namespace) n (fn [payload])))

(defn view
  "View messages in dead-set without removing them"
  [connection namespace n]
  (log/debugf "Getting %d number of messages from dead-letter-queue for topic %s" n namespace)
  (get-dead-set-messages connection (dead-set-queue namespace) n))
