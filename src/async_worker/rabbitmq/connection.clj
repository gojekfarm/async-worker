(ns async-worker.rabbitmq.connection
  (:require [clojure.tools.logging :as log]
            [langohr.core :as rmq])
  (:import [com.rabbitmq.client ShutdownListener Address ListAddressResolver]))

(defn start-connection
  "Starts a rabbitmq connection
   config expects:
   :hosts
   :port
   :username
   :password
   :admin-port
   :connection-timeout
   :automatically-recover
   :executor (optional) - ExecutorService to use for subscriber callbacks"
  [config]
  (log/info "Connecting to RabbitMQ")
  (let [connection (rmq/connect config)]
    (doto connection
      (.addShutdownListener
       (reify ShutdownListener
         (shutdownCompleted [_ cause]
           (when-not (.isInitiatedByApplication cause)
             (log/error cause "RabbitMQ connection shut down due to error"))))))))

(defn stop-connection [conn]
  (rmq/close conn)
  (log/info "Disconnected from RabbitMQ"))
