(ns strategy-engine.state
  "Redis access via carmine. Side effects live here."
  (:require [clojure.string]
            [taoensso.carmine :as car]))

(defn conn-opts
  "Build carmine connection spec from REDIS_URL (redis://host:port)."
  [redis-url]
  (let [stripped (clojure.string/replace (or redis-url "redis://localhost:6379")
                                         #"^redis://" "")
        [host port] (clojure.string/split stripped #":")]
    {:pool {} :spec {:host (or host "localhost")
                     :port (if port (Integer/parseInt port) 6379)}}))

(defn read-strategy
  "Read grid:strategy:{jobId} (JSON string or nil)."
  [conn job-id]
  (car/wcar conn (car/get (str "grid:strategy:" job-id))))

(defn write-strategy!
  "Write StrategyJSON string to grid:strategy:{jobId}."
  [conn job-id strategy-json]
  (car/wcar conn (car/set (str "grid:strategy:" job-id) strategy-json)))

(defn read-job
  "Read grid:job:{jobId} (JSON string or nil)."
  [conn job-id]
  (car/wcar conn (car/get (str "grid:job:" job-id))))

(defn list-jobs
  "List job ids from the grid:jobs hash."
  [conn]
  (or (car/wcar conn (car/hkeys "grid:jobs")) []))

(defn dead-end-set-size
  [conn job-id]
  (or (car/wcar conn (car/scard (str "grid:deadends:" job-id))) 0))

(defn xread-telemetry
  "XREAD new telemetry entries from grid:telemetry:{jobId} after `last-id`
  (\"$\" for only-new, \"0\" for all). Returns vector of [entry-id field-map]
  where field-map has string keys. Block `block-ms` milliseconds (0 = no block)."
  [conn job-id last-id block-ms]
  ;; NB: carmine's redis-call takes a single argument vector, and its raw
  ;; reply parser interns stream field names/values as symbols. We normalize
  ;; field keys to strings via `name` so downstream lookups ("score" etc.) work.
  (let [args (into ["XREAD"]
                   (concat (when (pos? (or block-ms 0)) ["BLOCK" (str block-ms)])
                           ["STREAMS" (str "grid:telemetry:" job-id) (or last-id "$")]))
        resp (car/wcar conn (car/redis-call args))]
    (if-let [stream (some-> resp first second)]
      (mapv (fn [[entry-id fields]]
              [(name entry-id)
               (into {}
                     (comp (partition-all 2)
                           (map (fn [[k v]] [(name k) v])))
                     fields)])
            stream)
      [])))

(defn heartbeat!
  "Write/refresh this node's entry in the grid:state hash."
  [conn node-json]
  (car/wcar conn (car/hset "grid:state" "strategy-engine" node-json)))
