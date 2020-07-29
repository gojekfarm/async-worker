(ns async-worker.core
  (:require [async-worker.rabbitmq.connection :as conn]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.producer :as producer]
            [async-worker.rabbitmq.consumer :as consumer]
            [async-worker.rabbitmq.dead-set :as dead-set]
            [async-worker.handler :as handler]))

(defonce rmq-conn (atom nil))

(defn enqueue
  ([connection handler args]
   (enqueue connection handler args false))
  ([connection handler args retry?]
   (producer/enqueue connection
                     "trips"
                     (merge args
                            {::handler/handler handler}
                            (when retry?
                              {::handler/retry? true
                               ::handler/retry-n 0})))))

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
  (enqueue @rmq-conn :prn {:hello :world})
  (enqueue @rmq-conn :prn {:hello :world2} true)
  (let [config {:worker-count 1
                :prefetch-count 1
                :queue-name "trips"
                :retry-count 2
                :retry-timeout-ms 100}]
    (consumer/start-subscribers @rmq-conn (handler/execute-with-retry @rmq-conn config) config))

  (dead-set/view @rmq-conn "trips" 10)
  (dead-set/delete @rmq-conn "trips" 1)
  (dead-set/replay @rmq-conn "trips" 3))
