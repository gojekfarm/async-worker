(ns async-worker.core
  (:require [async-worker.handler :as handler]
            [async-worker.rabbitmq.channel :as channel]
            [async-worker.rabbitmq.cluster :as cluster]
            [async-worker.rabbitmq.connection :as conn]
            [async-worker.rabbitmq.consumer :as consumer]
            [async-worker.rabbitmq.dead-set :as dead-set]
            [async-worker.rabbitmq.exchange :as exchange]
            [async-worker.rabbitmq.producer :as producer]
            [async-worker.rabbitmq.queue :as queue]
            [async-worker.spec :as spec]))

(def default-min-channels 10)

(defn start [{:keys [namespace rabbitmq executor jobs] :as config}]
  (spec/validate-config config)
  (let [rabbitmq-conn (conn/start-connection
                       (merge rabbitmq {:executor executor :automatically-recover true}))
        channel-pool  (channel/create-pool rabbitmq-conn
                                           (or (:channel-pool-size rabbitmq)
                                               default-min-channels)
                                           {:return-listener (:return-listener rabbitmq)})
        exchange-name (exchange/name namespace)]
    (exchange/declare rabbitmq-conn exchange-name)
    (queue/init rabbitmq-conn namespace exchange-name)
    (doseq [[job-name job-config] jobs]
      (when (:retry-max job-config)
        (queue/setup-delay-queues rabbitmq-conn
                                  namespace
                                  exchange-name
                                  (:retry-max job-config)
                                  (:retry-timeout-ms job-config))))
    (consumer/start-subscribers rabbitmq-conn
                                namespace
                                (:subscriber-count rabbitmq)
                                (handler/execute-with-retry channel-pool namespace jobs))
    (cluster/set-ha-policy rabbitmq namespace)
    (assoc config
           :connection rabbitmq-conn
           :channel-pool channel-pool)))

(defn stop [worker]
  (conn/stop-connection (:connection worker)))

(defn enqueue [config job-name args]
  (let [confirm-publish? (get-in config [:rabbitmq :enable-confirms])
        job-config (get-in config [:jobs job-name])]
    (producer/enqueue (:channel-pool config)
                      (:namespace config)
                      {:job-name          job-name
                       :args              args
                       :current-iteration 0
                       :retry-max         (:retry-max job-config)
                       :retry-timeout-ms  (:retry-timeout-ms job-config)}
                      confirm-publish?)))

(defn dead-set:view [config n]
  (dead-set/view (:connection config) (:namespace config) n))

(defn dead-set:replay [config n]
  (dead-set/replay (:connection config) (:namespace config) n))

(defn dead-set:delete [config n]
  (dead-set/delete (:connection config) (:namespace config) n))
