(ns async-worker.rabbitmq.connection
  (:require [clojure.tools.logging :as log]
            [langohr.core :as rmq])
  (:import [com.rabbitmq.client ShutdownListener Address ListAddressResolver]))

(defn start-connection
  "Starts a rabbitmq connection
   config expects:
   :host
   :port
   :username
   :password
   :channel-timeout
   :executor (optional) - ExecutorService to use for subsriber callbacks"
  [config error-handler]
  (log/info "Connecting to RabbitMQ")
  (try
    (let [connection (rmq/connect config)]
      (doto connection
        (.addShutdownListener
         (reify ShutdownListener
           (shutdownCompleted [_ cause]
             (when-not (.isInitiatedByApplication cause)
               (log/error cause "RabbitMQ connection shut down due to error")))))))
    (catch Exception e
      (error-handler e "Error while starting RabbitMQ connection")
      (throw e))))

(defn stop-connection [conn]
  (rmq/close conn)
  (log/info "Disconnected from RabbitMQ"))
