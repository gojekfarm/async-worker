# async-worker

An asynchronous job worker library for Clojure

## Installation

## Usage

## Examples

Hello world example:

``` clojure
(ns your.ns
  (:require [async-worker.core :as async-worker]))

(defn fn-1 [args]
  (prn {:my-fn args :at (System/currentTimeMillis)})
  (throw (Exception. "Retry")))

(defn fn-2 [payload]
  (prn {:my-fn-2 payload :retry :no})
  (throw (Exception. "Fail and move to dead set")))

(def executor (java.util.concurrent.Executors/newFixedThreadPool 5))

(def config {:namespace "trips"
             :rabbitmq  {:hosts              ["localhost"]
                         :port               5672
                         :username           "guest"
                         :password           "guest"
                         :admin-port         15672
                         :connection-timeout 2000
                         :subscriber-count   5}
             :executor  executor
             :jobs      {:job-1 {:retry-max        5
                                 :retry-timeout-ms 1000
                                 :handler-fn       fn-1}
                         :job-2 {:handler-fn fn-2}}})

(defonce worker (atom nil))

;; start and store the worker config/connection
(reset! worker (async-worker/start config))

(async-worker/enqueue @worker :job-1 {:hello :world-1})
(async-worker/enqueue @worker :job-2 {:hello :world-2})

;; stop the worker
(async-worker/stop @worker)
```


## Contributing
