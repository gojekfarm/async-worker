(ns async-worker.core
  (:require [async-worker.rabbitmq.connection :as conn]
            [async-worker.rabbitmq.queue :as q]
            [async-worker.rabbitmq.producer :as producer]
            [async-worker.rabbitmq.consumer :as consumer]
            [async-worker.rabbitmq.dead-set :as dead-set]
            [async-worker.handler :as handler]
            [async-worker.rabbitmq.exchange :as e]))

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

(defn new-conn [{:keys [host port username password connection-timeout] :as config}]
  (conn/start-connection {:host host
                          :port            port
                          :username        username
                          :password        password
                          :connection-timeout connection-timeout
                          :automatically-recover true}
                         prn))

(defn new-local-conn []
  (conn/start-connection {:host "localhost"
                          :port            5672
                          :username        "guest"
                          :password        "guest"
                          :connection-timeout 2000
                          :automatically-recover true
                          }
                         prn))



;{:namespace  "trips"
; :connection {:host               "localhost"
;              :port               5672
;              :username           "guest"
;              :password           "guest"
;              :connection-timeout 2000}
; :worker     {:count          5
;              :executor       executor
;              :prefetch-count 1}
; :jobs       {:auto-arrival {:retry-count      5
;                             :retry-timeout-ms 100
;                             :handler          handler-fn
;                             }}}


(defn init [{:keys [namespace connection worker jobs] :as config}]
  (let [rabbitmq-conn (new-conn config)
        exchange (e/exchange namespace)]
    (e/declare-exchange rabbitmq-conn exchange)
    (doseq [job jobs]
      (let [{:keys [retry-count retry-timeout-ms]} job]
        (q/create-and-bind-queues rabbitmq-conn {:namespace               namespace
                                          :base-expiration-time-ms retry-timeout-ms
                                          :max-retry-count         retry-count
                                          :exchange exchange})))
    (assoc connection
           :connection rabbitmq-conn)))

(comment
  #_(conn/stop-connection @rmq-conn)
  (reset! rmq-conn (new-local-conn))
  (def namespace "trips")
  (e/declare-exchange @rmq-conn (e/exchange namespace))
  (def retry-timeout-ms 100)
  (def retry-count 5)
  (def exchange (e/exchange namespace))
  (q/create-and-bind-queues @rmq-conn {:namespace               namespace
                                           :base-expiration-time-ms retry-timeout-ms
                                           :max-retry-count         retry-count
                                           :exchange exchange})
  ;; routing-key message
  (def que (q/queue namespace :instant))
  (def dque (q/delay-queue namespace 100))
  ;; :retry-n :max-retry-count :base-expiration-time :namespace :handler :message
  (def msg {:a "A" :b [1 2 3 4] :c {:d :e :f "G"}})
  (defn message [msg] {:retry-n 1 :max-retry-count 3 :base-expiration-time 1 :namespace "trips" :handler "my-job" :message msg})
  (producer/enqueue @rmq-conn exchange dque {:msg "Hello World"})
  ;(enqueue @rmq-conn :prn {:hello :world})
  ;(enqueue @rmq-conn :prn {:hello :world2} true)
  (defn my-fn [payload] (prn (str "my-fn : " payload)))
  (defn my-fn-2 [payload] (prn (str "my-fn-2 : " payload)))

  (def config {:namespace "trips"
               :worker-count 5
               :executor nil                                ;; TO DO
               :jobs {:my-job {:retry-max 5
                               :retry-timeout 1
                               :handler my-fn}
                      :my-job-2 {:retry-max 5
                                 :retry-timeout 2
                                 :handler my-fn-2}}
               })
  (let [config {:worker-count 1
                :prefetch-count 1
                :queue-name "trips"
                :retry-count 2
                :retry-timeout-ms 100}]
    (consumer/start-subscribers @rmq-conn (handler/execute-with-retry @rmq-conn config) config))

  (dead-set/view @rmq-conn "trips" 10)
  (dead-set/delete @rmq-conn "trips" 1)
  (dead-set/replay @rmq-conn "trips" 3))
