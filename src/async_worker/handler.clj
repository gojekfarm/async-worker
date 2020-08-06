(ns async-worker.handler
  (:require [clojure.tools.logging :as log]
            [async-worker.rabbitmq.producer :as producer]))

(defn- default-handler [{:keys [job-name args] :as message}]
  (log/error "Failed to find a handler for %s with args: %s" job-name args)
  (throw (ex-info "Failed to find job handler" {:message message})))

(defn- handler [jobs job-name]
  (get-in jobs [job-name :handler-fn]))

(defn execute-with-retry [connection namespace jobs]
  (fn [{:keys [args job-name retry-max current-iteration retry-timeout-ms] :as message}]
    (try
      (if-let [handler-fn (handler jobs job-name)]
        (handler-fn args)
        (default-handler message))
      (catch Exception e
        (if (and retry-max (< current-iteration retry-max))
          (producer/requeue connection
                            namespace
                            (update message :current-iteration inc)
                            current-iteration
                            retry-timeout-ms)
          (producer/move-to-dead-set connection namespace message))))))
