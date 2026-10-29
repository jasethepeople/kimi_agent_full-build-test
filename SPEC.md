# SPEC — Autonomous Distributed Hyperparameter Optimization & Processing Grid

Single source of truth. All subagents implement to these contracts EXACTLY.

## 0. Overview
Users submit compute workloads (HPO search jobs or shardable batch jobs) via REST.
The Go orchestrator shards the job into task chunks, pushes them onto Redis queues,
and provisions isolated Docker workers. Workers pull tasks, execute the workload,
publish results + telemetry. A Clojure Strategy & Critic Engine consumes telemetry
from the mesh, detects stagnation/plateaus, evolves new parameter strategies via a
genetic algorithm, and broadcasts mutated strategies over NATS. Workers and the
orchestrator apply strategy updates live.

## 1. Repository layout
```
hpo-grid/
├── README.md
├── Makefile
├── docker-compose.yml
├── .env.example
├── SPEC.md
├── orchestrator/                 # Go module: github.com/hpo-grid/orchestrator
│   ├── go.mod
│   ├── cmd/orchestrator/main.go
│   ├── internal/api/             # REST handlers
│   ├── internal/provision/       # Docker SDK worker lifecycle
│   ├── internal/queue/           # Redis task router
│   ├── internal/state/           # Redis state graph
│   ├── internal/mesh/            # NATS client (strategy updates, events)
│   └── internal/model/           # shared structs (mirror of section 4)
├── strategy-engine/              # Clojure (deps.edn)
│   ├── deps.edn
│   ├── src/strategy_engine/{core,telemetry,genetic,mesh,state}.clj
│   └── test/strategy_engine/{telemetry_test,genetic_test}.clj
├── worker/
│   ├── Dockerfile
│   ├── entrypoint.sh
│   ├── requirements.txt          # (python worker; no external deps beyond redis+nats-py? NO — stdlib+redis-py only)
│   └── worker.py
└── scripts/
    ├── smoke_test.sh
    └── seed_job.json
```

## 2. Configuration (environment variables, shared across all components)
| Var | Default | Used by |
|---|---|---|
| REDIS_ADDR | localhost:6379 | Go |
| REDIS_URL | redis://localhost:6379 | Clojure, Python worker |
| NATS_URL | nats://localhost:4222 | all |
| API_ADDR | :8080 | Go |
| WORKER_IMAGE | hpo-grid/worker:latest | Go provisioner |
| WORKER_REPLICAS | 2 | Go provisioner |
| JOB_SHARD_SIZE | 10 | Go |
| WORKER_POLL_TIMEOUT_SEC | 5 | worker |
| STRATEGY_EVAL_INTERVAL_SEC | 10 | Clojure |
| STAGNATION_WINDOW | 5 | Clojure (consecutive eval windows without improvement) |
| GRID_ID | grid-0 | all (prefixes nothing; informational) |

## 3. Redis data structures (Layer 3)
All keys are plain (no keyspace prefix besides what's shown). JSON values unless noted.

| Key | Type | Writer | Reader | Meaning |
|---|---|---|---|---|
| grid:jobs | HASH jobID -> JobJSON | Go | Go, Clojure | job registry |
| grid:job:{jobId} | STRING JobJSON | Go | all | job record |
| grid:queue:{jobId} | LIST of TaskJSON | Go (LPUSH) | workers (BRPOP) | pending task chunks |
| grid:processing:{jobId} | HASH taskID -> TaskJSON | worker | worker, Go | in-flight (visibility) |
| grid:results:{jobId} | LIST of ResultJSON | worker (LPUSH) | Go, Clojure | completed results |
| grid:deadends:{jobId} | SET of paramHash strings | worker | workers, Clojure | dedup / dead-end registry |
| grid:best:{jobId} | STRING ResultJSON | worker (conditional) | all | best-so-far result |
| grid:telemetry:{jobId} | STREAM (XADD) entries {worker_id, task_id, score, elapsed_ms, status} | worker | Clojure (XREAD) | telemetry stream |
| grid:strategy:{jobId} | STRING StrategyJSON (current active strategy) | Clojure | workers, Go | active parameter strategy |
| grid:state | HASH nodeID -> NodeJSON | all (heartbeat) | Go, Clojure | global state graph (node registry) |
| grid:counters:{jobId} | HASH {submitted, completed, failed, deadends} | Go, worker (HINCRBY) | all | progress counters |

### Param hash (dedup): sha256 hex of canonical JSON of the task's `params` map
(keys sorted, compact separators). Workers check SISMEMBER grid:deadends:{jobId}
before executing; if present, skip and report status="duplicate".

## 4. Message schemas (JSON, camelCase fields)
Job (POST body / grid:job value):
```json
{"jobId":"string","type":"hpo|batch","payload":{"objective":"string","searchSpace":{"paramName":[low,high]},"data":["opaque task items for batch jobs"]},"shardSize":10,"maxWorkers":4,"status":"pending|running|completed|failed","createdAt":"RFC3339","strategyId":"gen-0"}
```
Task:
```json
{"taskId":"string","jobId":"string","shardIndex":0,"params":{"lr":0.01},"items":["..."],"strategyId":"gen-0","enqueuedAt":"RFC3339"}
```
Result:
```json
{"taskId":"string","jobId":"string","workerId":"string","status":"ok|failed|duplicate|deadend","score":0.0,"metrics":{"elapsedMs":0},"output":"string","strategyId":"gen-0","completedAt":"RFC3339"}
```
Strategy:
```json
{"strategyId":"gen-N","jobId":"string","generation":0,"params":{"paramName":0.5},"mutationReason":"init|stagnation|periodic","createdAt":"RFC3339"}
```
Telemetry stream entry fields: workerId, taskId, jobId, score, elapsedMs, status, strategyId, params (JSON object string — lets the strategy engine build a real GA population from evaluated points).
Node (grid:state value):
```json
{"nodeId":"string","role":"orchestrator|strategy-engine|worker","startedAt":"RFC3339","lastBeat":"RFC3339","meta":{}}
```

## 5. NATS subjects
| Subject | Publisher | Subscribers | Payload |
|---|---|---|---|
| grid.nodes.hello | every node at startup | all | NodeJSON |
| grid.jobs.submitted | Go | Clojure | JobJSON |
| grid.results.breakthrough | worker (new best) | Go, Clojure | ResultJSON |
| grid.deadends.share | worker | workers, Clojure | {"jobId","paramHash","taskId"} |
| grid.strategy.update.{jobId} | Clojure | Go, workers of that job | StrategyJSON |
| grid.telemetry.flush | worker | Clojure (optional fast path) | ResultJSON |

## 6. Component behavior contracts

### 6.1 Go orchestrator
- `POST /api/v1/jobs` — accepts Job (jobId optional: server assigns `job-<unixnano>` if absent).
  Validates type & payload; stores job; shards payload.data (batch) or generates initial
  candidate points from searchSpace via current strategy (hpo) into Tasks of shardSize;
  LPUSHes tasks; sets status=running; publishes grid.jobs.submitted; ensures worker pool
  via provisioner. Returns 201 with JobJSON.
- `GET /api/v1/jobs/{jobId}` — job + counters + best. 404 if unknown.
- `GET /api/v1/jobs/{jobId}/results` — list results.
- `GET /api/v1/jobs` — list all jobs.
- `GET /api/v1/nodes` — state graph snapshot.
- `GET /healthz` — 200 {"status":"ok"}.
- Provisioner: uses Docker SDK (github.com/docker/docker/client) to create/start
  WORKER_REPLICAS containers of WORKER_IMAGE with env REDIS_URL/NATS_URL/JOB_ID;
  names `hpo-worker-{jobId}-{i}`; idempotent (skip if exists); graceful no-op with a
  warning log when Docker socket is unavailable (grid still functional with external
  workers). RemoveContainers on job completion is NOT automatic.
- Subscribes grid.strategy.update.{jobId} -> writes strategy to grid:strategy:{jobId},
  and generates new HPO candidate tasks from the mutated strategy.
- Heartbeats grid:state every 10s.
- Queue router uses BLPOP-recovery: on startup, requeue stale entries in
  grid:processing older than 5 min (simple: on GET job, report only).

### 6.2 Clojure strategy engine
- Consumes grid:telemetry:* streams (XREAD, track last IDs), and subscribes
  grid.jobs.submitted + grid.strategy.update (for awareness).
- Every STRATEGY_EVAL_INTERVAL_SEC per active job: compute success rate, mean score,
  score trend over STAGNATION_WINDOW windows, dead-end count.
- Stagnation = best score not improved for STAGNATION_WINDOW consecutive evals.
- On stagnation: genetic mutation — population = last 10 strategies/results per job;
  tournament selection, uniform crossover, gaussian mutation (sigma=0.1 of range),
  clamped to searchSpace; produce StrategyJSON generation+1, write grid:strategy:{jobId},
  publish grid.strategy.update.{jobId}.
- Pure functions in telemetry.clj and genetic.clj MUST be unit-testable without Redis/NATS
  (side effects isolated in mesh.clj/state.clj). clojure.test coverage for stagnation
  detection, selection, crossover, mutation clamping.
- Deps: org.clojure/clojure 1.11.1, com.taoensso/carmine 3.3.2 (Redis),
  io.nats/jnats 2.17.6 (via deps), org.clojure/data.json.

### 6.3 Python worker (Alpine container)
- stdlib + redis-py + nats-py (requirements.txt pins redis==5.0.8, nats-py==2.9.0).
- Loop: BRPOP grid:queue:{JOB_ID} (or, if JOB_ID empty, iterate grid:jobs keys) ->
  move to grid:processing -> dedup check -> execute workload:
  * batch: map `objective` over items (built-in demo objectives: "hash" = sha256 of item,
    "sleep" = timed sleep, "count" = length). Score = number of items processed.
  * hpo: simulate objective evaluation: score = -(sum((p - 0.42)^2 for p in params.values()))
    + small deterministic jitter derived from paramHash (so it's reproducible).
  * dead-end: if score < -10 (config DEADEND_THRESHOLD) record dead-end, publish share.
- XADD telemetry, LPUSH result, HINCRBY counters, conditional best update (score > current best), publish breakthrough on new best, heartbeat grid:state.
- Subscribes grid.strategy.update.{jobId}; on update, merges strategy params into
  subsequent task execution context (log it).
- Graceful SIGTERM handling: requeue in-flight task back to grid:queue.

### 6.4 Infra
- docker-compose.yml services: redis (redis:7-alpine, AOF on), nats (nats:2.10-alpine,
  -js), orchestrator (build ./orchestrator via Dockerfile), strategy-engine
  (build ./strategy-engine via Dockerfile clojure:temurin-21-tools-deps), worker
  (build ./worker, scale via --scale or WORKER_REPLICAS). Named network `grid`.
  Healthchecks for redis & nats. depends_on with condition: service_healthy.
- orchestrator/Dockerfile: multi-stage golang:1.22-alpine build -> alpine runtime.
- strategy-engine/Dockerfile: clojure tools-deps image, `clj -M -m strategy-engine.core`.
- Makefile targets: build, up, down, logs, test-go, test-clj, smoke, clean.
- scripts/smoke_test.sh: waits for healthz, POSTs seed_job.json, polls results until
  >=N results or timeout, prints best. Runnable with `make smoke`.
- .env.example with section 2 vars.

## 7. Testing requirements
- Go: `go test ./...` must pass; cover sharding, param-hash, API handler (httptest with
  miniredis), strategy update application. Use github.com/alicebob/miniredis/v2 v2.33.0.
- Clojure: `clj -M:test` runs clojure.test suites for telemetry + genetic pure fns.
- Worker: `python3 -m py_compile worker.py` + functions importable; keep I/O behind
  functions so a `main()` guard exists.
- Integration smoke (by main agent, local without Docker): run redis-server +
  nats-server + `go run` orchestrator + `python3 worker/worker.py` + `clj -M -m
  strategy-engine.core`, POST a job, verify results & strategy updates.

## 7.1 Implementation amendments (ratified after integration review)
- **Go module dependencies**: the build environment had no access to
  proxy.golang.org, so the orchestrator is stdlib-only: `internal/redisclient`
  (minimal RESP2 client) and `internal/natsclient` (minimal NATS core client)
  replace go-redis/nats.go behind clean interfaces; the provisioner speaks the
  Docker Engine API over /var/run/docker.sock instead of docker/docker/client;
  tests use an in-memory `FakeClient` instead of miniredis. Functional behavior
  per sections 3–6 is unchanged.
- **Duplicate results** increment BOTH `deadends` and `completed` counters (a
  duplicate is a finished task).
- **POST /api/v1/jobs** with a client-supplied jobId that already exists
  returns 409 Conflict.
- redisclient retries a command once after an I/O error; commands like LPUSH
  are not idempotent, so a lost reply can in rare cases duplicate a task.
  Accepted and documented; dedup via deadends plus visibility tracking
  bound the impact.

## 8. Non-negotiables
- No placeholders, no TODO stubs; complete runnable code.
- JSON field names exactly as in section 4 (Go struct tags must match).
- Every component must degrade gracefully when a dependency is briefly unavailable
  (retry with backoff, never crash-loop without logging).
