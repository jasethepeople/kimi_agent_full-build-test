(ns strategy-engine.telemetry
  "Pure telemetry aggregation functions. No I/O."
  (:require [clojure.data.json]))

(def success-statuses #{"ok"})

(defn- ->double
  "Coerce numbers, strings and symbols (carmine raw replies) to double."
  [v]
  (cond
    (nil? v) nil
    (number? v) (double v)
    :else (Double/parseDouble (str v))))

(defn- ->long
  [v]
  (cond
    (nil? v) nil
    (number? v) (long v)
    :else (Long/parseLong (str v))))

(defn- ->str
  [v]
  (when (some? v) (str v)))

(defn- ->params
  "Parse the `params` field: a JSON object string (as XADDed by the worker)
  into a {string double} map, or nil."
  [v]
  (when (and v (not= "" (str v)))
    (try
      (let [m (clojure.data.json/read-str (str v))]
        (into {} (map (fn [[k x]] [(name k) (->double x)])) m))
      (catch Throwable _ nil))))

(defn parse-entry
  "Normalize a telemetry entry map (string keys, from a Redis stream or JSON)
  into {:score double :status string :strategyId string :params map}."
  [entry]
  {:score (->double (get entry "score"))
   :elapsedMs (->long (get entry "elapsedMs"))
   :status (->str (get entry "status"))
   :strategyId (->str (get entry "strategyId"))
   :taskId (->str (get entry "taskId"))
   :params (->params (get entry "params"))})

(defn success-rate
  "Fraction of entries with status \"ok\". 0.0 for empty collections."
  [entries]
  (if (empty? entries)
    0.0
    (let [ok (count (filter #(contains? success-statuses (:status %)) entries))]
      (double (/ ok (count entries))))))

(defn mean-score
  "Mean of entry scores, ignoring nil scores. nil when no scores present."
  [entries]
  (let [scores (keep :score entries)]
    (when (seq scores)
      (double (/ (reduce + scores) (count scores))))))

(defn best-score
  "Best (max) score among entries. Higher is better. nil when empty."
  [entries]
  (let [scores (keep :score entries)]
    (when (seq scores)
      (reduce max scores))))

(defn dead-end-count
  "Number of entries with status \"deadend\" (failed and duplicate included via
  deadends set size when supplied instead)."
  [entries]
  (count (filter #(= "deadend" (:status %)) entries)))

(defn score-trend
  "Split entries into `n` chronological windows (by :at index order) and return
  the mean score per window (nil for empty windows)."
  [n entries]
  (let [v (vec entries)
        k (count v)
        size (max 1 (long (Math/ceil (/ (double k) (double (max 1 n))))))]
    (->> (range n)
         (mapv (fn [i]
                 (mean-score (subvec v (min k (* i size)) (min k (* (inc i) size)))))))))

(defn stagnant?
  "Stagnation = best score has not improved for `window` consecutive evals.
  `best-history` is a chronological vector of best scores observed at each
  eval tick (nil allowed for ticks with no data). Returns true when the last
  `window` observations contain no improvement over the first of them."
  [window best-history]
  (let [h (vec (remove nil? best-history))]
    (if (< (count h) window)
      false
      (let [recent (subvec h (- (count h) window))]
        (<= (reduce max recent) (first recent))))))

(defn window-summary
  "Aggregate a window of telemetry entries into a summary map."
  [entries]
  {:count (count entries)
   :successRate (success-rate entries)
   :meanScore (mean-score entries)
   :bestScore (best-score entries)
   :deadEnds (dead-end-count entries)})
