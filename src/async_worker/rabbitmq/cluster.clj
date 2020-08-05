(ns async-worker.rabbitmq.cluster
  (:require [async-worker.utils :as retry]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.http :as lh]
            [langohr.queue :as lq]))

(defn get-default-ha-policy [cluster-config]
  (let [ha-mode      (get cluster-config :ha-mode "all")
        ha-params    (get cluster-config :ha-params 1)
        ha-sync-mode (get cluster-config :ha-sync-mode "automatic")]
    (if (= "all" ha-mode)
      {:ha-mode ha-mode :ha-sync-mode ha-sync-mode}
      {:ha-mode ha-mode :ha-sync-mode ha-sync-mode :ha-params ha-params})))

(defn set-ha-policy [queue-name exchange-name cluster-config]
  (let [hosts-vec (s/split (:hosts cluster-config) #",")
        hosts     (atom hosts-vec)]
    (retry/with-retry {:count      (count @hosts)
                       :wait       50}
      (let [host (first @hosts)
            _    (swap! hosts rest)
            ha-policy (get-default-ha-policy cluster-config)]
        (binding [lh/*endpoint* (str "http://" host ":" (get cluster-config :admin-port 15672))
                  lh/*username* (:username cluster-config)
                  lh/*password* (:password cluster-config)]
          (log/info "applying HA policies to queue: " queue-name)
          (log/info "applying HA policies to exchange: " exchange-name)
          (lh/set-policy "/" (str queue-name "_ha_policy")
                         {:apply-to   "all"
                          :pattern    (str "^" queue-name "|" exchange-name "$")
                          :definition ha-policy}))))))
