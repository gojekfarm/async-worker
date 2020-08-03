(ns async-worker.handler
  (:require [clojure.tools.logging :as log]
            [async-worker.rabbitmq.producer :as producer]
            [async-worker.rabbitmq.exchange :as exc]))

;(defmulti execute
;  "Multimethod for defining execute handlers for enqueues jobs
;   Jobs are dispatched based on the :handler value in params "
;  (fn [params] (::handler params)))
;
;(defmethod execute :default [{handler ::handler :as params}]
;  (prn (str params))
;  (log/error (format "Failed to execute job for %s. No handler registered. params: %s" handler params))
;  (throw (ex-info "Failed to execute job" params)))
;
;;enqueue [connection exchange routing-key meta message]
;
;(defn execute-with-retry [connection {:keys [namespace retry-timeout-ms retry-count] :as config}]
;  (fn [{retry? ::retry? retry-n ::retry-n :as job-params}]
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
