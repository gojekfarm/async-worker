(ns async-worker.rabbitmq.channel
  (:require [langohr.channel :as lch]
            [pool.core :as pool]))

(defn create-pool [connection min-dile]
  (doto (pool/get-pool (fn [] (lch/open connection))
                       :validate lch/open? :destroy lch/close)
    (.setMinIdle min-dile)))

(defmacro with-channel
  [pool [channel] & body]
  `(let [~channel (pool/borrow ~pool)]
     (try
       (do ~@body)
       (finally
         (pool/return ~pool ~channel)))))
