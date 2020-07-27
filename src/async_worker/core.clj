(ns async-worker.core
  (:require [async-worker.rabbitmq.connection :as conn]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.producer :as producer]
            [async-worker.rabbitmq.consumer :as consumer]
            [async-worker.handler :as handler]))

(defonce rmq-conn (atom nil))

(defn enqueue [args retry?])

(defmethod handler/execute :prn
  [params]
  (prn :executing-with-params params))

(defn new-conn []
  (conn/start-connection {:host "localhost"
                          :port            5672
                          :prefetch-count  3
                          :username        "guest"
                          :password        "guest"
                          :channel-timeout 2000}
                         prn))

(comment
  #_(conn/stop-connection @rmq-conn)
  (reset! rmq-conn (new-conn))
  (queue/make-queues @rmq-conn {:queue-name "trips" :retry-count 2})
  (producer/publish-to-instant-queue @rmq-conn "trips" {:hello :world
                                                        ::handler/retry-n 0
                                                        ::handler/handler :prn})
  (producer/publish-to-instant-queue @rmq-conn "trips" {:hello :world2
                                                        ::handler/retry? true
                                                        ::handler/retry-n 0
                                                        ::handler/handler :prn})
  (let [config {:worker-count 1
                :prefetch-count 1
                :queue-name "trips"
                :retry-count 2
                :retry-timeout-ms 100}]
    (consumer/start-subscribers @rmq-conn (handler/execute-with-retry @rmq-conn config) config)))
