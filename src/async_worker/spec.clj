(ns async-worker.spec
  (:require [clojure.spec.alpha :as s])
  (:import [java.util.concurrent Executor]))

(s/def ::namespace string?)

(s/def ::hosts (s/coll-of string?))
(s/def ::port int?)
(s/def ::username string?)
(s/def ::password string?)
(s/def ::admin-port int?)
(s/def ::connection-timeout int?)
(s/def ::subscriber-count int?)
(s/def ::enable-confirms boolean?)
(s/def ::channel-pool-size int?)
(s/def ::rabbitmq
  (s/keys :req-un [::hosts
                   ::port
                   ::username
                   ::password
                   ::admin-port
                   ::connection-timeout
                   ::subscriber-count]
          :opt-un [::enable-confirms
                   ::channel-pool-size]))

(s/def ::job-name keyword?)
(s/def ::handler-fn fn?)
(s/def ::retry-max int?)
(s/def ::retry-timeout-ms int?)
(s/def ::job-config
  (s/keys :req-un [::handler-fn]
          :opt-un [::retry-max
                   ::retry-timeout-ms]))
(s/def ::jobs
  (s/map-of ::job-name ::job-config))

(defn- executor? [e]
  (instance? Executor e))
(s/def ::executor executor?)

(s/def ::config
  (s/keys :req-un [::namespace
                   ::rabbitmq
                   ::jobs]
          :opt-un [::executor]))

(defn validate-config [config]
  (s/check-asserts true)
  (s/assert ::config config))
