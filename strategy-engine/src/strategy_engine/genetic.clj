(ns strategy-engine.genetic
  "Pure genetic algorithm functions: tournament selection, uniform crossover,
  gaussian mutation, strategy evolution. No I/O.")

(defn clamp
  "Clamp x into [low high]."
  [low high x]
  (-> x (max low) (min high)))

(defn- gaussian
  "Sample from standard normal distribution (Box-Muller)."
  [rng]
  (let [u1 (max 1.0e-12 (.nextDouble rng))
        u2 (.nextDouble rng)]
    (* (Math/sqrt (* -2.0 (Math/log u1)))
       (Math/cos (* 2.0 Math/PI u2)))))

(defn tournament-select
  "Tournament selection: pick `k` random individuals (with score), return the one
  with the highest :score. `population` is a seq of maps each having :score and
  :params."
  ([population] (tournament-select population 3 (java.util.Random.)))
  ([population k rng]
   (let [v (vec population)]
     (when (empty? v)
       (throw (ex-info "tournament-select requires a non-empty population" {})))
     (->> (repeatedly (min k (count v)) #(nth v (.nextInt rng (count v))))
          (apply max-key #(or (:score %) Double/NEGATIVE_INFINITY))))))

(defn uniform-crossover
  "Uniform crossover between two params maps over the given searchSpace keys:
  each gene taken from either parent with p=0.5. If a parent lacks a gene,
  the other parent's value (or the space midpoint) is used."
  ([params-a params-b search-space]
   (uniform-crossover params-a params-b search-space (java.util.Random.)))
  ([params-a params-b search-space rng]
   (into {}
         (map (fn [[param [low high]]]
                (let [va (get params-a param)
                      vb (get params-b param)
                      fallback (/ (+ (double low) (double high)) 2.0)
                      choose-a? (< (.nextDouble rng) 0.5)
                      v (if choose-a?
                          (or va vb fallback)
                          (or vb va fallback))]
                  [param v])))
         search-space)))

(defn gaussian-mutate
  "Gaussian mutation: for each gene, with probability `rate`, add N(0, sigma)
  noise where sigma = 0.1 * (high - low). Result clamped to searchSpace bounds."
  ([params search-space]
   (gaussian-mutate params search-space 0.2 (java.util.Random.)))
  ([params search-space rate rng]
   (into {}
         (map (fn [[param [low high]]]
                (let [low (double low)
                      high (double high)
                      sigma (* 0.1 (- high low))
                      base (double (or (get params param) (/ (+ low high) 2.0)))
                      mutated (if (< (.nextDouble rng) rate)
                                (+ base (* sigma (gaussian rng)))
                                base)]
                  [param (clamp low high mutated)])))
         search-space)))

(defn evolve
  "Evolve a population of scored individuals ({:score :params} maps) into the
  next StrategyJSON map (camelCase string keys). `current` is the active
  StrategyJSON map (string keys) used for strategyId/generation base.
  search-space is {\"param\" [low high]}. mutation-reason is a string."
  ([population current search-space]
   (evolve population current search-space "stagnation" (java.util.Random.)))
  ([population current search-space mutation-reason]
   (evolve population current search-space mutation-reason (java.util.Random.)))
  ([population current search-space mutation-reason rng]
   (let [pop (vec population)
         gen (long (or (get current "generation") 0))
         job-id (get current "jobId")
         parent-a (tournament-select pop 3 rng)
         parent-b (tournament-select pop 3 rng)
         child (uniform-crossover (:params parent-a) (:params parent-b)
                                  search-space rng)
         mutated (gaussian-mutate child search-space 0.2 rng)]
     {"strategyId" (str "gen-" (inc gen))
      "jobId" job-id
      "generation" (inc gen)
      "params" mutated
      "mutationReason" mutation-reason
      "createdAt" (.toString (java.time.Instant/ofEpochMilli (System/currentTimeMillis)))})))
