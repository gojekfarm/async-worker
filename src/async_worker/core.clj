(ns async-worker.core
  (:require [async-worker.rabbitmq.connection :as conn]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.rabbitmq.producer :as producer]
            [async-worker.rabbitmq.consumer :as consumer]
            [async-worker.rabbitmq.dead-set :as dead-set]
            [async-worker.handler :as handler]
            [async-worker.rabbitmq.exchange :as exchange])
  (:import [java.util.concurrent Executors]))

(defonce rmq-conn (atom nil))

(defn new-conn [{:keys [host port username password connection-timeout executor] :as config}]
  (conn/start-connection {:host                  host
                          :port                  port
                          :username              username
                          :password              password
                          :connection-timeout    connection-timeout
                          :executor              executor
                          :automatically-recover true}))

(defn init [{:keys [namespace rabbitmq executor jobs] :as config}]
  (let [rabbitmq-conn (new-conn (assoc rabbitmq :executor executor))
        exchange-name (exchange/name namespace)]
    (exchange/declare rabbitmq-conn exchange-name)
    (queue/init rabbitmq-conn namespace exchange-name)
    (doseq [[job-name job-config] jobs]
      (queue/setup-delay-queues rabbitmq-conn
                                namespace
                                exchange-name
                                (:retry-max job-config)
                                (:retry-timeout-ms job-config)))
    (consumer/start-subscribers rabbitmq-conn
                                namespace
                                (:subscriber-count rabbitmq)
                                (handler/execute-with-retry rabbitmq-conn namespace jobs))
    (assoc config :connection rabbitmq-conn)))


(defn enqueue [config job-name args]
  (let [job-config (get-in config [:jobs job-name])]
    (producer/enqueue (:connection config)
                      (:namespace config)
                      {:job-name          job-name
                       :args              args
                       :current-iteration 0
                       :retry-max         (:retry-max job-config)
                       :retry-timeout-ms  (:retry-timeout-ms job-config)})))

(comment
  (defn my-fn [payload]
    (prn {:my-fn payload :at (System/currentTimeMillis)})
    (throw (Exception. "Retry")))
  (defn my-fn-2 [payload]
    (prn {:my-fn-2 payload}))

  (def config {:namespace "trips"
             :rabbitmq  {:host               "localhost"
                         :port               5672
                         :username           "guest"
                         :password           "guest"
                         :connection-timeout 2000
                         :subscriber-count   5}
             :executor   (Executors/newFixedThreadPool 5)
             :jobs      {:my-job   {:retry-max        5
                                    :retry-timeout-ms 1000
                                    :handler-fn          my-fn}
                         :my-job-2 {:retry-max     5
                                    :retry-timeout-ms 2
                                    :handler-fn       my-fn-2}}})



  (defonce worker (atom nil))
  (reset! worker (init config))
  #_(conn/stop-connection (:connection @worker))

  (enqueue @worker :my-job {:hello :world}))
