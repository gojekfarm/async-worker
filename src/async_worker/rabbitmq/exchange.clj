(ns async-worker.rabbitmq.exchange
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [taoensso.nippy :as nippy]
            [camel-snake-kebab.core :as csk])
  (:refer-clojure :exclude [name]))

(defn declare [connection exchange]
  (with-open [ch (lch/open connection)]
    (le/direct ch exchange {:durable true :auto-delete false})
    (log/info "Declared exchange - " exchange)))

(defn name [namespace]
  (apply format "%s_exchange" (map csk/->snake_case_string [namespace])))
