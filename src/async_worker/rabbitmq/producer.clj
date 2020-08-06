(ns async-worker.rabbitmq.producer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.confirm :as lconf]
            [taoensso.nippy :as nippy]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.exchange :as exchange]
            [async-worker.utils :as u]))

(def confirmation-timeout-ms 1000)

(defn- properties-for-publish
  [expiration]
  (let [props {:content-type "application/octet-stream"
               :persistent   true
               :mandatory    true
               :headers      {}}]
    (if expiration
      (assoc props :expiration (str expiration))
      props)))

(defn- die-if-message-returned [p]
  (when (realized? p)
    (throw (ex-info "Message returned during publish" @p))))

(defn- publish
  "Tries to publish the message reliably. Retries 5 times with 100ms wait; throws exception if unsuccessful

  Unrouted messges are returned with ack and are handled by the return-handler.

  Nack-ed messages are handled by wait-for-confirms-or-die.
  Throws exception if the messages are not acked within `confirmation-timeout-ms`"
  ([connection exchange routing-key message-payload]
   (publish connection exchange routing-key message-payload nil))
  ([connection exchange routing-key message-payload expiration]
   (u/with-retry {:count 5 :wait 100}
     (let [return-promise (promise)
           return-handler (lb/return-listener (fn [reply-code reply-text exchange routing-key properties body]
                                                (deliver return-promise {:reply-code reply-code
                                                                         :reply-text reply-text})))]
       (with-open [ch (lch/open connection)]
         (.addReturnListener ch return-handler)
         (lconf/select ch)
         (lb/publish ch
                     exchange
                     routing-key
                     (nippy/freeze message-payload)
                     (properties-for-publish expiration))
         (lconf/wait-for-confirms-or-die ch confirmation-timeout-ms)
         (die-if-message-returned return-promise))))))

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
