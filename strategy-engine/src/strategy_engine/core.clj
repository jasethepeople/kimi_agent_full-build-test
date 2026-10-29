(ns strategy-engine.core
  "Strategy & Critic Engine main loop."
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [strategy-engine.telemetry :as telemetry]
            [strategy-engine.genetic :as genetic]
            [strategy-engine.mesh :as mesh]
            [strategy-engine.state :as state]))

(def redis-url (or (System/getenv "REDIS_URL") "redis://localhost:6379"))
(def nats-url (or (System/getenv "NATS_URL") "nats://localhost:4222"))
(def eval-interval-sec
  (long (or (some-> (System/getenv "STRATEGY_EVAL_INTERVAL_SEC") Long/parseLong) 10)))
(def stagnation-window
  (long (or (some-> (System/getenv "STAGNATION_WINDOW") Long/parseLong) 5)))

(defonce runtime
  (atom {:jobs {}           ; jobId -> {:last-id string :best-history [scores] :job map}
         :redis nil
         :nats nil
         :running? true}))

(defn- now-rfc3339 []
  (.toString (java.time.Instant/ofEpochMilli (System/currentTimeMillis))))

(defn- parse-json [s]
  (when (and s (not (str/blank? s)))
    (json/read-str s)))

(defn node-json []
  (json/write-str {"nodeId" "strategy-engine"
                   "role" "strategy-engine"
                   "startedAt" (or (:started-at @runtime) (now-rfc3339))
                   "lastBeat" (now-rfc3339)
                   "meta" {"gridId" (or (System/getenv "GRID_ID") "grid-0")}}))

(defn register-job!
  "Register a job from JobJSON map for tracking."
  [job]
  (when-let [job-id (get job "jobId")]
    (swap! runtime assoc-in [:jobs job-id]
           (merge (get-in @runtime [:jobs job-id])
                  {:job job :last-id (or (get-in @runtime [:jobs job-id :last-id]) "0")
                   :best-history (or (get-in @runtime [:jobs job-id :best-history]) [])}))
    (println "strategy-engine: tracking job" job-id)))

(defn refresh-jobs!
  "Discover jobs from Redis registry (in case we missed the NATS message)."
  []
  (doseq [job-id (state/list-jobs (:redis @runtime))]
    (when-not (contains? (:jobs @runtime) job-id)
      (when-let [job (parse-json (state/read-job (:redis @runtime) job-id))]
        (register-job! job)))))

(defn- strategy-population
  "Population for evolution: params from recent telemetry (last 10 scored ok
  entries) plus the current strategy."
  [conn job-id search-space current-strategy]
  (let [entries (state/xread-telemetry conn job-id "0" 0)
        scored (->> entries
                    (map second)
                    (map telemetry/parse-entry)
                    (filter #(and (:score %) (= "ok" (:status %))))
                    (take-last 10)
                    (mapv (fn [e]
                            {:score (:score e)
                             :params (or (:params e)
                                         (into {}
                                               (map (fn [[p [lo hi]]] [p (/ (+ (double lo) (double hi)) 2.0)]))
                                               search-space))})))]
    (if (seq scored)
      scored
      [{:score Double/NEGATIVE_INFINITY
        :params (or (get current-strategy "params")
                    (into {}
                          (map (fn [[p [lo hi]]] [p (/ (+ (double lo) (double hi)) 2.0)]))
                          search-space))}])))

(defn evaluate-job!
  "One eval tick for a job: read new telemetry, update best-history, check
  stagnation, and on stagnation evolve + persist + publish a new strategy."
  [job-id]
  (let [{:keys [redis nats]} @runtime
        job-state (get-in @runtime [:jobs job-id])
        last-id (:last-id job-state "0")
        new-entries (state/xread-telemetry redis job-id last-id 0)
        entries (mapv (comp telemetry/parse-entry second) new-entries)
        new-last-id (if-let [lid (ffirst (take-last 1 new-entries))] lid last-id)
        best (telemetry/best-score entries)
        best-so-far (let [prev (:best-so-far job-state)]
                      (cond
                        (and best prev) (max best prev)
                        best best
                        :else prev))
        ;; SPEC: stagnation is measured over consecutive EVAL TICKS, so the
        ;; history records the best-so-far at every tick, even when no new
        ;; telemetry arrived (nil-safe: stagnant? drops leading nils).
        history (conj (vec (:best-history job-state [])) best-so-far)
        summary (telemetry/window-summary entries)
        dead-ends (+ (long (:deadEnds summary)) (long (state/dead-end-set-size redis job-id)))]
    ;; update-in (not assoc of a previously captured job-state) so a
    ;; concurrent register-job! from the NATS dispatcher cannot be clobbered.
    (swap! runtime update-in [:jobs job-id]
           (fn [s]
             (-> (or s {})
                 (assoc :last-id new-last-id
                        :best-history (conj (vec (:best-history s)) best-so-far)
                        :best-so-far (let [prev (:best-so-far s)]
                                       (cond
                                         (and best prev) (max best prev)
                                         best best
                                         :else prev))))))
    (println (format "strategy-engine: job %s eval: n=%d successRate=%.2f meanScore=%s best=%s deadEnds=%d"
                     job-id (:count summary) (double (:successRate summary))
                     (str (:meanScore summary)) (str (:bestScore summary)) dead-ends))
    (when (telemetry/stagnant? stagnation-window history)
      (let [job (:job job-state)
            search-space (get-in job ["payload" "searchSpace"] {})
            current (or (parse-json (state/read-strategy redis job-id))
                        {"strategyId" "gen-0" "jobId" job-id "generation" 0
                         "params" {} "mutationReason" "init"
                         "createdAt" (now-rfc3339)})
            population (strategy-population redis job-id search-space current)
            next-strategy (genetic/evolve population current search-space "stagnation")
            strategy-json (json/write-str next-strategy)]
        (state/write-strategy! redis job-id strategy-json)
        (mesh/publish-json! nats (str "grid.strategy.update." job-id) strategy-json)
        ;; give the freshly mutated strategy a clean window to prove itself
        (swap! runtime update-in [:jobs job-id]
               #(-> % (assoc :best-history []) (dissoc :best-so-far)))
        (println "strategy-engine: stagnation on" job-id
                 "-> evolved" (get next-strategy "strategyId"))))))

(defn eval-loop []
  (while (:running? @runtime)
    (try
      (refresh-jobs!)
      (doseq [job-id (keys (:jobs @runtime))]
        (evaluate-job! job-id))
      (state/heartbeat! (:redis @runtime) (node-json))
      (catch Throwable t
        (println "strategy-engine: eval loop error:" (.getMessage t))))
    (Thread/sleep (* 1000 eval-interval-sec))))

(defn- connect-with-backoff [what f]
  (loop [attempt 1]
    (or (try (f) (catch Throwable t
                   (println "strategy-engine:" what "connect failed (attempt" attempt "):" (.getMessage t))
                   nil))
        (do (Thread/sleep (min 30000 (* 1000 (long (Math/pow 2 (min attempt 5))))))
            (recur (inc attempt))))))

(defn -main [& _]
  (println "strategy-engine: starting (redis" redis-url "nats" nats-url ")")
  (swap! runtime assoc :started-at (now-rfc3339))
  (let [redis (state/conn-opts redis-url)
        _ (connect-with-backoff "redis"
            #(state/heartbeat! redis (node-json)))
        nats (connect-with-backoff "nats" #(mesh/connect nats-url))]
    (swap! runtime assoc :redis redis :nats nats)
    (mesh/subscribe! nats "grid.jobs.submitted"
                     (fn [payload]
                       (when-let [job (parse-json payload)]
                         (register-job! job))))
    ;; awareness of strategy updates circulating in the mesh (SPEC 6.2)
    (mesh/subscribe! nats "grid.strategy.update.*"
                     (fn [payload]
                       (when-let [st (parse-json payload)]
                         (println "strategy-engine: observed strategy update"
                                  (get st "strategyId") "for" (get st "jobId")))))
    (mesh/publish-json! nats "grid.nodes.hello" (node-json))
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable
                               (fn []
                                 (swap! runtime assoc :running? false)
                                 (mesh/close! (:nats @runtime)))))
    (eval-loop)))
