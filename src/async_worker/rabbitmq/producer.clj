(ns async-worker.rabbitmq.producer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [taoensso.nippy :as nippy]
            [async-worker.rabbitmq.exchange :as exc]
            [async-worker.rabbitmq.retry :refer [with-retry]]))

(defn- publish
  ([connection exchange routing-key payload]
   (try
     (with-retry {:count 5 :wait 100}
       (with-open [ch (lch/open connection)]
         (lb/publish ch
                     exchange
                     routing-key
                     (nippy/freeze payload)
                     {:content-type "application/octet-stream"
                      :persistent   true})))
     (catch Throwable e
       (log/error "Publishing message to rabbitmq failed with error " (.getMessage e))
       (throw e)
       ))))

(comment
  "
  message -> [meta, msg]
  meta -> [retry info, handler info]


  "
  )





(defn enqueue [connection exchange routing-key message]
  ;; TO DO : add routing logic
  (publish connection exchange routing-key message))

;(defn move-to-dead-set [connection exchange message]
;  ;; TO DO : adding routing logic
;  (publish connection (exc/exchange queue-name) message))

;(defn- backoff-duration [retry-n timeout-ms]
;  (-> (Math/pow 2 retry-n)
;      (* timeout-ms)
;      int))

;(defn requeue [connection queue-name message retry-n retry-timeout-ms]
;  ;; TO DO : add routing logic
;  (publish connection
;           (exc/exchange queue-name)
;           message
;           (backoff-duration retry-n retry-timeout-ms)))
