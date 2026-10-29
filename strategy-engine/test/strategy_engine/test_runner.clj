(ns strategy-engine.test-runner
  "Self-contained test runner: clj -M:test"
  (:require [clojure.test :as t]
            [strategy-engine.telemetry-test]
            [strategy-engine.genetic-test]))

(defn -main [& _]
  (let [{:keys [fail error]}
        (t/run-tests 'strategy-engine.telemetry-test
                     'strategy-engine.genetic-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
