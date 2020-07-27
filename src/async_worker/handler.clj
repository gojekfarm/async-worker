(ns async-worker.handler
  (:require [clojure.tools.logging :as log]
            [async-worker.rabbitmq.producer :as producer]))

(defmulti execute
  "Multimethod for defining execute handlers for enqueues jobs
   Jobs are dispatched based on the :handler value in params "
  (fn [params] (::handler params)))

(defmethod execute :default [{handler ::handler :as params}]
  (log/error (format "Failed to execute job for %s. No handler registered. params: %s" handler params))
  (throw (ex-info "Failed to execute job" params)))

(defn execute-with-retry [connection {:keys [queue-name retry-timeout-ms retry-count] :as config}]
  (fn [{retry? ::retry? retry-n ::retry-n :as job-params}]
    (try
      (execute job-params)
      (catch Exception e
        (if (and retry? (< retry-n retry-count))
          (producer/requeue connection
                            queue-name
                            (update job-params ::retry-n inc)
                            retry-n
                            retry-timeout-ms)
          (producer/publish-to-dead-queue connection queue-name job-params))))))
