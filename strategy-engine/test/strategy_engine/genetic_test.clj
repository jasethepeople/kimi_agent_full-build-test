(ns strategy-engine.genetic-test
  (:require [clojure.test :refer [deftest is testing]]
            [strategy-engine.genetic :as ga]))

(def space {"lr" [0.0001 1.0] "depth" [1 16]})

(def population
  [{:score 0.9 :params {"lr" 0.5 "depth" 8}}
   {:score 0.1 :params {"lr" 0.001 "depth" 2}}
   {:score 0.5 :params {"lr" 0.1 "depth" 4}}
   {:score -1.0 :params {"lr" 0.9 "depth" 12}}])

(deftest clamp-test
  (is (= 0.0 (ga/clamp 0.0 1.0 -3.0)))
  (is (= 1.0 (ga/clamp 0.0 1.0 5.0)))
  (is (= 0.5 (ga/clamp 0.0 1.0 0.5))))

(deftest tournament-select-test
  (testing "returns a member of the population"
    (dotimes [_ 20]
      (let [winner (ga/tournament-select population 3 (java.util.Random. 42))]
        (is (contains? (set population) winner)))))
  (testing "full-population tournament always returns the best"
    (let [winner (ga/tournament-select population 4 (java.util.Random. 1))]
      (is (= 0.9 (:score winner)))))
  (testing "throws on empty population"
    (is (thrown? clojure.lang.ExceptionInfo
                 (ga/tournament-select [] 3 (java.util.Random.))))))

(deftest uniform-crossover-test
  (testing "offspring genes come from parents' gene pool"
    (let [a {"lr" 0.2 "depth" 3}
          b {"lr" 0.7 "depth" 10}
          child (ga/uniform-crossover a b space (java.util.Random. 7))]
      (is (contains? #{(a "lr") (b "lr")} (child "lr")))
      (is (contains? #{(a "depth") (b "depth")} (child "depth")))
      (is (= (set (keys space)) (set (keys child))))))
  (testing "missing gene falls back to the other parent"
    (let [child (ga/uniform-crossover {"lr" 0.2} {"lr" 0.7 "depth" 10}
                                      space (java.util.Random. 3))]
      (is (= 10 (child "depth"))))))

(deftest gaussian-mutate-test
  (testing "mutation stays clamped to searchSpace bounds"
    (dotimes [seed 50]
      (let [mutated (ga/gaussian-mutate {"lr" 0.9999 "depth" 15.9} space 1.0
                                        (java.util.Random. seed))]
        (is (<= 0.0001 (mutated "lr") 1.0))
        (is (<= 1.0 (mutated "depth") 16.0)))))
  (testing "zero rate leaves params unchanged"
    (let [m (ga/gaussian-mutate {"lr" 0.5 "depth" 8} space 0.0 (java.util.Random. 1))]
      (is (= 0.5 (m "lr")))
      (is (= 8.0 (m "depth"))))))

(deftest evolve-test
  (let [current {"strategyId" "gen-0" "jobId" "job-1" "generation" 0
                 "params" {"lr" 0.5 "depth" 8} "mutationReason" "init"}
        next (ga/evolve population current space "stagnation" (java.util.Random. 11))]
    (is (= "gen-1" (get next "strategyId")))
    (is (= "job-1" (get next "jobId")))
    (is (= 1 (get next "generation")))
    (is (= "stagnation" (get next "mutationReason")))
    (is (string? (get next "createdAt")))
    (is (every? (fn [[p [lo hi]]] (<= (double lo) (get-in next ["params" p]) (double hi)))
                space))))
