(ns async-worker.fixtures
  (:require [async-worker.rabbitmq.connection :as connection]
            [async-worker.rabbitmq.exchange :as exchange]
            [langohr.queue :as lq]
            [langohr.exchange :as le]
            [langohr.channel :as lch]
            [clojure.string :as s]
            [async-worker.rabbitmq.channel :as channel]))

(def ^:dynamic *connection* nil)

(def ^:dynamic *channel-pool* nil)

(def ^:dynamic *exchange* nil)

(def ^:dynamic *queue* nil)

(defn rmq-host []
  (let [env-val (System/getenv (str "RABBITMQ_HOST"))]
    (if-not (s/blank? env-val)
      env-val
      "localhost")))

(def connection-config
  {:hosts              [(rmq-host)]
   :port               5672
   :username           "guest"
   :password           "guest"
   :admin-port         15672
   :connection-timeout 2000})

(defn get-connection [] *connection*)

(defn get-channel-pool [] *channel-pool*)

(defn get-exchange [] *exchange*)

(defn get-queue [] *queue*)

(defn delete-queue [q]
  (with-open [ch (lch/open (get-connection))]
    (lq/delete ch q)))

(defn delete-exchange [e]
  (with-open [ch (lch/open (get-connection))]
    (le/delete ch e)))

(defn with-rabbitmq-connection [f]
  (binding [*connection* (connection/start-connection connection-config)]
    (f)
    (connection/stop-connection *connection*)))

(defn with-channel-pool [f]
  (binding [*channel-pool* (channel/create-pool (get-connection) 5 {})]
    (f)))

(defn with-rabbitmq-exchange [f]
  (let [exchange-name (str (gensym "async-test-exchange-"))]
    (exchange/declare (get-connection) exchange-name)
    (binding [*exchange* exchange-name]
      (f)
      (delete-exchange exchange-name))))

(defn with-bound-queue [f]
  (let [queue-name (str (gensym "async-test-queue-"))]
    (with-open [ch (lch/open (get-connection))]
      (lq/declare ch queue-name {:durable false :auto-delete true})
      (lq/bind ch queue-name (get-exchange))
      (binding [*queue* queue-name]
        (f)
        (delete-queue queue-name)))))
