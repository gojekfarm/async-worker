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
      [(consumer/convert-message payload) (:delivery-tag meta)])))

(defn- reject-all [ch delivery-tags]
  (run! #(lb/reject ch % true) (remove nil? delivery-tags)))

(defn- get-dead-set-messages
  [connection queue-name n]
  (with-open [ch (lch/open connection)]
    (let [messages-and-tags (mapv (fn [_] (read-without-ack ch queue-name)) (range n))]
      (reject-all ch (map second messages-and-tags))
      (remove nil? (map first messages-and-tags)))))

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
  (process-dead-set-messages connection (dead-set-queue namespace) n
                             (fn [payload] (log/debugf "Deleting from dead-letter-queue for %s: %s" namespace payload))))

(defn view
  "View messages in dead-set without removing them"
  [connection namespace n]
  (log/debugf "Getting %d number of messages from dead-letter-queue for topic %s" n namespace)
  (get-dead-set-messages connection (dead-set-queue namespace) n))
