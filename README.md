# hpo-grid

An autonomous, distributed hyperparameter optimization (HPO) and batch-processing
grid. Jobs are submitted over REST, sharded by a Go orchestrator into task chunks
backed by Redis queues, and executed by isolated Python workers. A Clojure
Strategy & Critic Engine watches the telemetry stream, detects search stagnation,
evolves improved parameter strategies with a genetic algorithm, and broadcasts
them over NATS for live application by workers and the orchestrator.

The full interface contract is in [SPEC.md](SPEC.md); it is the single source of
truth.

## Architecture

Four layers:

1. **Interface layer** — REST API exposed by the Go orchestrator.
2. **Compute layer** — Python worker containers that pull and execute task shards.
3. **Collaboration fabric** — Redis (queues, state graph, telemetry stream, best
   results, dead-end registry) and NATS JetStream (events and strategy updates).
4. **Intelligence layer** — the Clojure strategy engine (telemetry analysis +
   genetic strategy evolution).

```
                 +------------------------------------------+
   HTTP/curl --> |  Layer 1: Go orchestrator (:8080)        |
                 |  REST API, sharding, queue router,       |
                 |  worker provisioner (Docker SDK)         |
                 +----+------------------+------------------+
                      |                  |
            LPUSH tasks|                  |sub grid.strategy.update.*
                      v                  v
   +----------------------------------------------------------+
   |             Layer 3: Collaboration fabric                 |
   |                                                           |
   |  Redis                        NATS (-js)                  |
   |   grid:queue:{jobId} (LIST)    grid.jobs.submitted        |
   |   grid:results:{jobId}         grid.results.breakthrough  |
   |   grid:telemetry:{jobId} (STREAM) grid.strategy.update.*  |
   |   grid:best / deadends / counters / state  grid.deadends..|
   +------^--------------------^-------------^-----------^----+
          | BRPOP             | XREAD       | pub/sub
          | XADD              |             |
+---------+--------+  +-------+-------+  +--+----------------+
| Layer 2: workers |  | Layer 2: ...  |  | Layer 4: Clojure  |
| (python, xN,     |  | (scaled pool) |  | strategy engine   |
|  docker --scale) |  +---------------+  | stagnation detect |
+------------------+                     | genetic evolution |
                                         +-------------------+
```

Flow: `POST /api/v1/jobs` -> orchestrator validates, shards, LPUSHes tasks to
`grid:queue:{jobId}`, provisions workers -> workers BRPOP tasks, dedup against
`grid:deadends:{jobId}`, execute, XADD telemetry, LPUSH results, update
`grid:best:{jobId}` on improvement -> strategy engine consumes telemetry, and on
stagnation mutates the strategy and publishes `grid.strategy.update.{jobId}` ->
orchestrator generates fresh candidate tasks from the mutated strategy and
workers merge the new parameters into their execution context.

## Quickstart

```sh
cp .env.example .env          # optional; defaults work out of the box
docker compose up --build -d  # build images and start redis, nats, orchestrator,
                              # strategy-engine, and a baseline worker
make smoke                    # end-to-end check: submit seed job, wait for results
```

Scale the baseline worker pool:

```sh
docker compose up -d --scale worker=4
# or
make up WORKERS=4
```

The orchestrator also provisions additional workers itself through the Docker
socket (mounted read-only into the orchestrator container) up to each job's
`maxWorkers`; when the socket is unavailable it logs a warning and the grid
keeps working with externally provided workers.

Stop everything:

```sh
make down       # keep volumes
make clean      # also remove volumes and locally built images
```

## API reference

Base URL: `http://localhost:8080`

| Method & path | Description |
|---|---|
| `GET /healthz` | Liveness probe. Returns `200 {"status":"ok"}`. |
| `POST /api/v1/jobs` | Submit a Job. Returns `201` with the stored Job JSON. `jobId` is optional — the server assigns `job-<unixnano>`. |
| `GET /api/v1/jobs/{jobId}` | Job record plus `counters` and `best`. `404` if unknown. |
| `GET /api/v1/jobs/{jobId}/results` | List of Result JSON for the job. |
| `GET /api/v1/jobs` | List all jobs. |
| `GET /api/v1/nodes` | Snapshot of the global state graph (node heartbeats). |

### Examples

Submit an HPO job (see `scripts/seed_job.json`):

```sh
curl -sS -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
        "type": "hpo",
        "payload": {
          "objective": "demo",
          "searchSpace": {"lr": [0.0001, 1.0], "momentum": [0.0, 1.0]}
        },
        "shardSize": 10,
        "maxWorkers": 4
      }'
```

Submit a batch job:

```sh
curl -sS -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
        "type": "batch",
        "payload": {
          "objective": "hash",
          "data": ["alpha", "bravo", "charlie", "delta"]
        },
        "shardSize": 2,
        "maxWorkers": 2
      }'
```

Poll a job (returns the job, progress counters, and the best result so far):

```sh
curl -sS http://localhost:8080/api/v1/jobs/job-1718123400000000000 | jq .
```

List results and inspect the grid's nodes:

```sh
curl -sS http://localhost:8080/api/v1/jobs/job-1718123400000000000/results | jq .
curl -sS http://localhost:8080/api/v1/nodes | jq .
```

## Redis key reference

All values are JSON unless noted (SPEC section 3).

| Key | Type | Meaning |
|---|---|---|
| `grid:jobs` | HASH jobID -> JobJSON | Job registry |
| `grid:job:{jobId}` | STRING JobJSON | Job record |
| `grid:queue:{jobId}` | LIST TaskJSON | Pending task shards (LPUSH / BRPOP) |
| `grid:processing:{jobId}` | HASH taskID -> TaskJSON | In-flight tasks |
| `grid:results:{jobId}` | LIST ResultJSON | Completed results |
| `grid:deadends:{jobId}` | SET paramHash | Dead-end / duplicate registry |
| `grid:best:{jobId}` | STRING ResultJSON | Best result so far |
| `grid:telemetry:{jobId}` | STREAM | Per-task telemetry (XADD/XREAD) |
| `grid:strategy:{jobId}` | STRING StrategyJSON | Active parameter strategy |
| `grid:state` | HASH nodeID -> NodeJSON | Node registry / heartbeats |
| `grid:counters:{jobId}` | HASH {submitted, completed, failed, deadends} | Progress counters |

The param hash used for dedup is the sha256 hex of the canonical JSON of a
task's `params` map (sorted keys, compact separators).

## NATS subject reference

| Subject | Publisher -> Subscribers | Payload |
|---|---|---|
| `grid.nodes.hello` | every node -> all | NodeJSON at startup |
| `grid.jobs.submitted` | orchestrator -> strategy engine | JobJSON |
| `grid.results.breakthrough` | worker -> orchestrator, strategy engine | ResultJSON (new best) |
| `grid.deadends.share` | worker -> workers, strategy engine | `{"jobId","paramHash","taskId"}` |
| `grid.strategy.update.{jobId}` | strategy engine -> orchestrator, workers of the job | StrategyJSON |
| `grid.telemetry.flush` | worker -> strategy engine (optional fast path) | ResultJSON |

## Environment variables

See `.env.example` for a ready-to-copy file (SPEC section 2).

| Var | Default | Used by |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | Go orchestrator |
| `REDIS_URL` | `redis://localhost:6379` | Clojure engine, Python worker |
| `NATS_URL` | `nats://localhost:4222` | all components |
| `API_ADDR` | `:8080` | Go orchestrator |
| `WORKER_IMAGE` | `hpo-grid/worker:latest` | Go provisioner |
| `WORKER_REPLICAS` | `2` | Go provisioner |
| `JOB_SHARD_SIZE` | `10` | Go sharding default |
| `WORKER_POLL_TIMEOUT_SEC` | `5` | worker BRPOP timeout |
| `STRATEGY_EVAL_INTERVAL_SEC` | `10` | Clojure eval loop period |
| `STAGNATION_WINDOW` | `5` | eval windows without improvement before mutation |
| `GRID_ID` | `grid-0` | informational grid identifier (all components) |

`docker-compose.yml` additionally honours `REDIS_PORT`, `NATS_PORT`, and
`NATS_MONITOR_PORT` for the host-side published ports.

## How strategy mutation works

Every `STRATEGY_EVAL_INTERVAL_SEC` seconds the Clojure engine evaluates each
active job using its telemetry stream: success rate, mean score, score trend,
and dead-end count. If the best score has not improved for `STAGNATION_WINDOW`
consecutive evaluations, the search is considered stagnant and the genetic
algorithm runs:

1. **Population** — the last 10 strategies/results recorded for the job.
2. **Selection** — tournament selection picks parents by score.
3. **Crossover** — uniform crossover mixes parent parameter vectors.
4. **Mutation** — gaussian perturbation with sigma = 0.1 of each parameter's
   range.
5. **Clamping** — offspring are clamped back into the job's `searchSpace`.

The engine emits a `StrategyJSON` with `generation` incremented and
`mutationReason: "stagnation"`, writes it to `grid:strategy:{jobId}`, and
publishes it on `grid.strategy.update.{jobId}`. The orchestrator persists it and
generates new HPO candidate tasks from the mutated parameters; workers merge the
strategy params into their subsequent execution context — the search adapts
without restarting anything.

## Testing

```sh
make test-go       # cd orchestrator && go test ./...
                   #   (sharding, param-hash, API handlers via httptest+miniredis,
                   #    strategy-update application)
make test-clj      # cd strategy-engine && clj -M:test
                   #   (pure telemetry + genetic fns: stagnation, selection,
                   #    crossover, mutation clamping)
make test-worker   # python3 -m py_compile worker/worker.py + import check
make smoke         # requires the compose stack running; submits scripts/seed_job.json,
                   # waits for completions (120s timeout), prints the best result
```

Smoke test knobs (environment): `API`, `TIMEOUT_SEC`, `EXPECTED_COMPLETED`,
`SEED_FILE`. Example:

```sh
EXPECTED_COMPLETED=10 TIMEOUT_SEC=180 make smoke
```

## Repo layout

```
hpo-grid/
├── README.md                 # this file
├── SPEC.md                   # interface contract (single source of truth)
├── Makefile                  # build / up / down / logs / ps / tests / smoke / clean
├── docker-compose.yml        # redis + nats + orchestrator + strategy-engine + worker
├── .env.example              # all SPEC section-2 variables with defaults
├── orchestrator/             # Go module github.com/hpo-grid/orchestrator
│   ├── cmd/orchestrator/     # main entrypoint
│   └── internal/             # api, provision, queue, state, mesh, model
├── strategy-engine/          # Clojure deps.edn project
│   ├── src/strategy_engine/  # core, telemetry, genetic, mesh, state
│   └── test/strategy_engine/ # telemetry_test, genetic_test
├── worker/                   # Python worker (Dockerfile, entrypoint, worker.py)
└── scripts/
    ├── seed_job.json         # demo HPO job used by the smoke test
    └── smoke_test.sh         # end-to-end smoke test
```

## How this system was built

This repository was produced by a multi-agent software-engineering swarm rather
than a single author. A coordinating agent distilled the requirements into
`SPEC.md` — a single, precise interface contract covering repository layout,
configuration, Redis data structures, message schemas, NATS subjects, component
behaviors, and testing requirements. Specialist agents then implemented each
layer in parallel against that contract in isolated git worktrees: one built the
Go orchestrator, one the Clojure strategy engine, one the Python worker, and one
the build/run infrastructure (this layer). Because every agent coded to the same
schemas, keys, and subjects, the independently written components integrate
without negotiation. A final integration pass runs the shared smoke test across
the assembled grid and reconciles any drift against the spec.

In practice the contract-first approach held, but the live integration pass
against real Redis and NATS servers still caught five cross-component bugs that
unit tests with fakes could not:

1. **Cross-language dedup hash drift** — Go's `encoding/json` renders integral
   floats as `1`, Python's `json.dumps` as `1.0`; the worker's canonical JSON
   now mirrors Go exactly, pinned by shared hash test vectors on both sides.
2. **Carmine raw-call misuse** — `redis-call` requires the full command as one
   argument vector and interns reply tokens as symbols; `xread-telemetry` now
   normalizes keys/values to strings.
3. **Stagnation semantics** — stagnation is defined per *eval tick*, not per
   new score; the engine records best-so-far every tick so an idle plateau is
   detected and triggers genetic mutation.
4. **Genetic `evolve` arity** — the core loop called a 4-arity overload that
   did not exist; added.
5. **NATS `MSG` frame parsing** — the minimal stdlib NATS client dropped
   messages without a reply-to field (4-token frames); fixed and covered by an
   in-process fake-server regression test.

The end-to-end run (orchestrator + strategy engine + worker against live Redis
and NATS) executed HPO and batch jobs, streamed telemetry, detected stagnation,
and autonomously evolved strategies across multiple generations, each consumed
by workers — the full feedback loop of the grid.
