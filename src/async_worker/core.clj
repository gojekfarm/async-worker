(ns async-worker.core
  (:require [async-worker.rabbitmq.connection :as conn]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.producer :as producer]
            [async-worker.rabbitmq.consumer :as consumer]))

(defonce rmq-conn (atom nil))

(defn new-conn []
  (conn/start-connection {:host "localhost"
                          :port            5672
                          :prefetch-count  3
                          :username        "guest"
                          :password        "guest"
                          :channel-timeout 2000}
                         prn))

(comment
  (reset! rmq-conn (new-conn))
  (queue/make-queues @rmq-conn {:queue-name "trips" :retry-count 2})
  (producer/publish-to-instant-queue @rmq-conn "trips" {:hello :world})
  (producer/publish-to-instant-queue @rmq-conn "trips" {:hello :world2})
  (consumer/start-subscribers @rmq-conn prn {:worker-count 1
                                             :prefetch-count 1
                                             :queue-name (rmq/queue "trips" :instant)}))
