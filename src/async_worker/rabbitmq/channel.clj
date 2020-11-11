(ns async-worker.rabbitmq.channel
  (:require [async-worker.rabbitmq.payload :as payload]
            [clojure.tools.logging :as log]
            [langohr.channel :as lch]
            [langohr.basic :as lb]
            [pool.core :as pool]))

(defn default-return-listener [arg]
  (log/warn "Message returned from rabbitmq" arg))

(defn- add-return-listener [channel return-listener-f]
  (lb/add-return-listener channel
                          (fn [reply-code reply-text exchange routing-key properties body]
                            (return-listener-f {:reply-code  reply-code
                                                :reply-text  reply-text
                                                :exchange    exchange
                                                :routing-key routing-key
                                                :properties  properties
                                                :body        (payload/decode body)}))))

(defn- create-channel [connection {:keys [return-listener] :as opts}]
  (let [channel (lch/open connection)]
    (add-return-listener channel (or return-listener default-return-listener))
    channel))

(defn create-pool [connection min-dile opts]
  (doto (pool/get-pool (fn [] (create-channel connection opts))
                       :validate lch/open? :destroy lch/close)
    (.setMinIdle min-dile)))

(defmacro with-channel
  [pool [channel] & body]
  `(let [~channel (pool/borrow ~pool)]
     (try
       (do ~@body)
       (finally
         (pool/return ~pool ~channel)))))
