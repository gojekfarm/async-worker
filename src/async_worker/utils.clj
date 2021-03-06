(ns async-worker.utils)

(defn with-retry* [retry-count wait fn-to-retry]
  (let [res (try
              (fn-to-retry)
              (catch Exception e
                (if-not (zero? retry-count)
                  ::try-again
                  (throw e))))]
    (if (= res ::try-again)
      (do
        (Thread/sleep (or wait 10))
        (recur (dec retry-count) wait fn-to-retry))
      res)))

(defmacro with-retry [{:keys [count wait]} & body]
  `(with-retry* ~count ~wait (fn [] ~@body)))

(defn backoff-duration [current-iteration timeout-ms]
  (-> (Math/pow 2 current-iteration)
      (* timeout-ms)
      int))
