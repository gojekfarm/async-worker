(ns async-worker.rabbitmq.payload
  (:require [taoensso.nippy :as nippy]))

(defn encode [x]
  (nippy/freeze x))

(defn decode [o]
  (nippy/thaw o))
