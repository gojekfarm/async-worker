(ns async-worker.smoke-test
  (:require [async-worker.core :as core]
            [async-worker.fixtures :as f]
            [clojure.test :refer :all]))

(deftest basic-features-test
  (let [config              {:namespace (str (gensym "async-test-"))
                             :rabbitmq  (merge f/connection-config
                                               {:subscriber-count  3
                                                :enable-confirms   true
                                                :channel-pool-size 5
                                                :return-listener (fn [&_])})
                             :jobs      {:retry-job   {:retry-max        3
                                                       :retry-timeout-ms 10
                                                       :handler-fn       identity}
                                         :regular-job {:handler-fn identity}}}
        retry-invocations   (atom [])
        retry-job-fn        (fn [msg]
                              (swap! retry-invocations conj [msg (System/currentTimeMillis)])
                              (throw (ex-info "Trigger retry" {})))
        regular-invocations (atom [])
        regular-job-fn      (fn [msg]
                              (swap! regular-invocations conj [msg (System/currentTimeMillis)])
                              (throw (ex-info "Trigger failure" {})))
        worker              (core/start (-> config
                                            (assoc-in [:jobs :retry-job :handler-fn] retry-job-fn)
                                            (assoc-in [:jobs :regular-job :handler-fn] regular-job-fn)))]
    (testing "jobs with retry are retried"
      (core/enqueue worker :retry-job {:hello :retry})
      (Thread/sleep 200)
      (is (= 4 (count @retry-invocations)))
      (is (= [{:job-name          :retry-job,
               :args              {:hello :retry},
               :current-iteration 3,
               :retry-max         3,
               :retry-timeout-ms  10}]
             (core/dead-set:view worker 1))))

    (testing "jobs without retry are executed and moved directly to dead set whey they fail"
      (core/enqueue worker :regular-job {:hello :regular})
      (Thread/sleep 200)
      (is (= 1 (count @regular-invocations)))
      (is (= {:job-name          :regular-job,
              :args              {:hello :regular},
              :current-iteration 0,
              :retry-max         nil,
              :retry-timeout-ms  nil}
             (last (core/dead-set:view worker 2)))))

    (core/stop worker)))
