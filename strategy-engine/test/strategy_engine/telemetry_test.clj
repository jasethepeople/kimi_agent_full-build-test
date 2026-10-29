(ns strategy-engine.telemetry-test
  (:require [clojure.test :refer [deftest is testing]]
            [strategy-engine.telemetry :as tel]))

(deftest success-rate-test
  (testing "empty collection yields 0.0"
    (is (= 0.0 (tel/success-rate []))))
  (testing "mixed statuses"
    (is (= 0.5 (tel/success-rate [{:status "ok"} {:status "failed"}
                                  {:status "ok"} {:status "deadend"}]))))
  (testing "all ok"
    (is (= 1.0 (tel/success-rate [{:status "ok"} {:status "ok"}])))))

(deftest mean-and-best-score-test
  (is (nil? (tel/mean-score [])))
  (is (nil? (tel/best-score [])))
  (is (= 2.0 (tel/mean-score [{:score 1.0} {:score 3.0} {:score nil}])))
  (is (= 3.0 (tel/best-score [{:score 1.0} {:score 3.0} {:score 2.5}]))))

(deftest dead-end-count-test
  (is (= 0 (tel/dead-end-count [{:status "ok"}])))
  (is (= 2 (tel/dead-end-count [{:status "deadend"} {:status "ok"} {:status "deadend"}]))))

(deftest stagnant-test
  (testing "not enough history is never stagnant"
    (is (false? (tel/stagnant? 5 [1.0 1.0 1.0 1.0]))))
  (testing "exactly window flat evals is stagnant"
    (is (true? (tel/stagnant? 5 [1.0 1.0 1.0 1.0 1.0]))))
  (testing "improvement within window breaks stagnation"
    (is (false? (tel/stagnant? 3 [0.5 0.5 0.5 1.0 1.0]))))
  (testing "stagnation after prior improvement then flat"
    (is (true? (tel/stagnant? 3 [1.0 2.0 2.0 2.0]))))
  (testing "declining scores count as no improvement"
    (is (true? (tel/stagnant? 3 [5.0 4.0 3.0 2.0]))))
  (testing "nil entries are ignored"
    (is (false? (tel/stagnant? 3 [nil 1.0 nil 2.0])))))

(deftest window-summary-test
  (let [s (tel/window-summary [{:score 1.0 :status "ok"}
                               {:score -1.0 :status "deadend"}])]
    (is (= 2 (:count s)))
    (is (= 0.5 (:successRate s)))
    (is (= 0.0 (:meanScore s)))
    (is (= 1.0 (:bestScore s)))
    (is (= 1 (:deadEnds s)))))

(deftest parse-entry-string-values-test
  ;; carmine returns stream fields as strings/symbols — parse-entry must coerce
  (let [e (tel/parse-entry {"score" "-0.5" "elapsedMs" "12"
                                  "status" "ok" "strategyId" "gen-1"
                                  "taskId" "t-1"
                                  "params" "{\"lr\":0.4,\"momentum\":0.9}"})]
    (is (= -0.5 (:score e)))
    (is (= 12 (:elapsedMs e)))
    (is (= "ok" (:status e)))
    (is (= {"lr" 0.4 "momentum" 0.9} (:params e))))
  (is (nil? (:params (tel/parse-entry {"score" "1" "params" ""}))))
  (is (nil? (:score (tel/parse-entry {"status" "duplicate"})))))
