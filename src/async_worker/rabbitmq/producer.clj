(ns async-worker.rabbitmq.producer
  (:require [async-worker.rabbitmq.channel :as channel]
            [async-worker.rabbitmq.exchange :as exchange]
            [async-worker.rabbitmq.payload :as payload]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.utils :as u]
            [langohr.basic :as lb]
            [langohr.confirm :as lconf]))

(def confirmation-timeout-ms 1000)

(def properties-for-publish
  {:content-type "application/octet-stream"
   :persistent   true
   :mandatory    true
   :headers      {}})

(defn publish
  "Tries to publish the message reliably. Retries 5 times with 100ms wait; throws exception if unsuccessful

  Unrouted messges are returned with ack and are handled by the return-handler.

  Nack-ed messages are handled by wait-for-confirms-or-die if publisher confirms are enabled.
  Throws exception if the messages are not acked within `confirmation-timeout-ms`"
  [channel-pool exchange routing-key message-payload confirm-publish?]
  (u/with-retry {:count 5 :wait 100}
    (channel/with-channel channel-pool [ch]
      (when confirm-publish?
        (lconf/select ch))
      (lb/publish ch
                  exchange
                  routing-key
                  (payload/encode message-payload)
                  properties-for-publish)
      (when confirm-publish?
        (lconf/wait-for-confirms-or-die ch confirmation-timeout-ms)))))

(defn enqueue
  ([channel-pool namespace message]
   (enqueue channel-pool namespace message false))
  ([channel-pool namespace message confirm-publish?]
   (publish channel-pool
            (exchange/name namespace)
            (queue/name namespace :instant)
            message
            confirm-publish?)))

(defn move-to-dead-set [channel-pool namespace message]
  (publish channel-pool
           (exchange/name namespace)
           (queue/name namespace :dead-letter)
           message
           true))

(defn requeue [channel-pool namespace message current-iteration retry-timeout-ms]
  (publish channel-pool
           (exchange/name namespace)
           (queue/delay-queue-name namespace (u/backoff-duration current-iteration retry-timeout-ms))
           message
           true))
