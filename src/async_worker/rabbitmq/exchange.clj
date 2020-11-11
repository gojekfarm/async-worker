(ns async-worker.rabbitmq.exchange
  (:refer-clojure :exclude [name declare])
  (:require [camel-snake-kebab.core :as csk]
            [clojure.tools.logging :as log]
            [langohr.channel :as lch]
            [langohr.exchange :as le]))

(defn declare [connection exchange]
  (with-open [ch (lch/open connection)]
    (le/direct ch exchange {:durable true :auto-delete false})
    (log/info "Declared exchange - " exchange)))

(defn name [namespace]
  (apply format "%s_exchange" (map csk/->snake_case_string [namespace])))
