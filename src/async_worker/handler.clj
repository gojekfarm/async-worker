(ns async-worker.handler
  (:require [clojure.tools.logging :as log]
            [async-worker.rabbitmq.producer :as producer]))

(defn- default-handler [{:keys [job-name args] :as message}]
  (log/errorf "Failed to find a handler for %s with args: %s" job-name args)
  (throw (ex-info "Failed to find job handler" {:message message}))
  :fail)

(defn- handler [jobs job-name]
  (get-in jobs [job-name :handler-fn]))

(defn process-result [channel-pool
                      namespace
                      {:keys [retry-max current-iteration retry-timeout-ms] :as message}
                      result]
  (cond
    (= :retry result)
    (if (and retry-max (< current-iteration retry-max))
      (producer/requeue channel-pool
                        namespace
                        (update message :current-iteration inc)
                        current-iteration
                        retry-timeout-ms)
      (producer/move-to-dead-set channel-pool namespace message))

    (= :fail result)
    (producer/move-to-dead-set channel-pool namespace message)

    :else
    :nil))

(defn execute-with-retry [channel-pool namespace jobs]
  (fn [{:keys [args job-name retry-max current-iteration retry-timeout-ms] :as message}]
    (let [handler-fn (handler jobs job-name)]
      (try
        (->> (if (some? handler-fn)
               (handler-fn args)
               (default-handler message))
             (process-result channel-pool namespace message))
        (catch Exception e
          (log/warnf e "Uncaught exception in job handler for %s" job-name)
          (process-result channel-pool namespace message :retry))))))
