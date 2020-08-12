# async-worker

An asynchronous job worker library for Clojure, implemented on RabbitMQ

## Installation

```clojure
[com.gojek/async-worker "0.0.1-SNAPSHOT"]
```

## Usage

Initialize workers with `async-worker.core/start`
(See below for example)

Use a common `namespace` to create a group of worker instances

If supplied, optional `executor` will be used instead of the rabbitmq default subscriber executor.

Omit `retry-max` and `retry-timeout-ms` if the retries are not required for a job.
Only exponential backoff strategy is available for retries.

job-names must be keywords. Handler functions should expect a single argument

`start` returns a map with a connection. Pass this map to all core functions as first argument.

`async-worker.core/enqueue` accepts the config, job-name and the single argument for handler function.

`enqueue` will wait upto 1000ms to ensure that the job was enqueued and will throw an exception if otherwise.

The core ns also contains `dead-set:view`,`dead-set:delete`, and `dead-set:replay` functions. Please note that replayed messages from dead-set are not retried during execution.

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
