(ns async-worker.handler-test
  (:require [async-worker.handler :as handler]
            [clojure.test :refer :all]
            [async-worker.rabbitmq.producer :as producer]))

(deftest execute-with-retry
  (testing "calls default handler and moves message to dead-set when no handler is available"
    (let [handler-fn        (handler/execute-with-retry nil nil {})
          default-called?   (atom false)
          moved-to-deadset? (atom false)
          original-fn       (var-get #'async-worker.handler/default-handler)]
      (with-redefs [async-worker.handler/default-handler (fn [args] (reset! default-called? true) (original-fn args))
                    producer/move-to-dead-set            (fn [& _] (reset! moved-to-deadset? true))]
        (handler-fn {:job-name :404})
        (is (true? @default-called?))
        (is (true? @moved-to-deadset?)))))

  (testing "calls the corresponding job handler-fn for the job"
    (let [job-fn-called? (atom false)
          job-fn         (fn [_] (reset! job-fn-called? true))
          handler-fn     (handler/execute-with-retry nil nil {:job {:handler-fn job-fn}})]
      (handler-fn {:job-name :job})
      (is (true? @job-fn-called?))))

  (testing "moves message to dead set if job handler throws exception"
    (let [job-fn            (fn [_] (throw (Exception. "test")))
          handler-fn        (handler/execute-with-retry nil nil {:job {:handler-fn job-fn}})
          moved-to-deadset? (atom false)]
      (with-redefs [producer/move-to-dead-set (fn [& _] (reset! moved-to-deadset? true))]
        (handler-fn {:job-name :job})
        (is (true? @moved-to-deadset?)))))

  (testing "moves message to dead set if job handler returns :fail"
    (let [job-fn            (fn [_] :fail)
          handler-fn        (handler/execute-with-retry nil nil {:job {:handler-fn job-fn}})
          moved-to-deadset? (atom false)]
      (with-redefs [producer/move-to-dead-set (fn [& _] (reset! moved-to-deadset? true))]
        (handler-fn {:job-name :job})
        (is (true? @moved-to-deadset?)))))

  (testing "requeues message if job handler returns :retry and retry is configured"
    (let [job-fn            (fn [_] :retry)
          handler-fn        (handler/execute-with-retry nil nil {:job {:handler-fn job-fn}})
          requeued-message (atom nil)]
      (with-redefs [producer/requeue (fn [_ _ msg _ _] (reset! requeued-message msg))]
        (handler-fn {:job-name :job :current-iteration 0 :retry-max 3})
        (is (= 1 (:current-iteration @requeued-message))))))

  (testing "moves message to deadset if job handler returns :retry and retry is exhausted"
    (let [job-fn            (fn [_] :retry)
          handler-fn        (handler/execute-with-retry nil nil {:job {:handler-fn job-fn}})
          moved-to-deadset? (atom false)]
      (with-redefs [producer/move-to-dead-set (fn [& _] (reset! moved-to-deadset? true))]
        (handler-fn {:job-name :job :current-iteration 3 :retry-max 3})
        (is (true? @moved-to-deadset?)))))

  (testing "retries message if job handler throws exception"
    (let [job-fn            (fn [_] (throw (Exception. "test")))
          handler-fn        (handler/execute-with-retry nil nil {:job {:handler-fn job-fn}})
          requeued-message (atom nil)]
      (with-redefs [producer/requeue (fn [_ _ msg _ _] (reset! requeued-message msg))]
        (handler-fn {:job-name :job :current-iteration 0 :retry-max 3})
        (is (= 1 (:current-iteration @requeued-message)))))))
