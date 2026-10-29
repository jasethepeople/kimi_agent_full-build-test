"""hpo-grid Layer 4: containerized execution worker.

Per SPEC.md section 6.3. Pure logic (param hash, scoring, objectives,
dead-end classification) is importable without redis/nats installed.
"""
import hashlib
import json
import logging
import os
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [worker] %(message)s",
)
log = logging.getLogger("worker")

# ---------------------------------------------------------------------------
# Configuration (SPEC section 2)
# ---------------------------------------------------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
JOB_ID = os.environ.get("JOB_ID", "")
POLL_TIMEOUT = int(os.environ.get("WORKER_POLL_TIMEOUT_SEC", "5"))
DEADEND_THRESHOLD = float(os.environ.get("DEADEND_THRESHOLD", "-10"))
GRID_ID = os.environ.get("GRID_ID", "grid-0")
WORKER_ID = os.environ.get(
    "WORKER_ID", "worker-%s-%d" % (socket.gethostname(), os.getpid())
)

# ---------------------------------------------------------------------------
# Pure logic (importable without redis/nats)
# ---------------------------------------------------------------------------


def _go_float_str(v):
    """Render a number exactly like Go's encoding/json renders float64:
    shortest round-trip form, integral floats without a trailing '.0'
    (e.g. 1.0 -> '1', 0.42 -> '0.42'). Python's repr() also produces
    shortest round-trip, so this matches Go for all typical values."""
    f = float(v)
    if f == int(f) and abs(f) < 1e15:
        return str(int(f))
    return repr(f)


def canonical_json(obj):
    """Canonical JSON: keys sorted, compact separators, Go-style float
    rendering (SPEC section 3). Cross-language contract with the Go
    orchestrator (internal/model.ParamHash): both sides must produce
    byte-identical canonical strings so dedup hashes agree."""
    if isinstance(obj, dict):
        items = sorted(obj.items(), key=lambda kv: kv[0])
        return "{" + ",".join(
            json.dumps(str(k)) + ":" + _go_float_str(v) for k, v in items
        ) + "}"
    return _go_float_str(obj)


def param_hash(params):
    """sha256 hex of canonical JSON of a params map (dedup key)."""
    if params is None:
        params = {}
    return hashlib.sha256(canonical_json(params).encode("utf-8")).hexdigest()


def hpo_score(params):
    """HPO simulated objective (SPEC 6.3).

    score = -(sum((p - 0.42)^2 for p in params.values()))
            + small deterministic jitter derived from paramHash (reproducible).
    """
    params = params or {}
    base = -sum((float(p) - 0.42) ** 2 for p in params.values())
    ph = param_hash(params)
    # deterministic jitter in [-0.05, 0.05) derived from the hash
    jitter = (int(ph[:8], 16) / 0xFFFFFFFF - 0.5) * 0.1
    return base + jitter


def run_batch_objective(objective, items):
    """Map a built-in demo objective over batch items.

    "hash"  -> sha256 hex of item
    "sleep" -> timed sleep (0.1s per item), returns seconds slept
    "count" -> length of item
    Returns (outputs list, elapsed_ms). Score = number of items processed.
    """
    outputs = []
    start = time.monotonic()
    for item in items or []:
        if objective == "hash":
            outputs.append(hashlib.sha256(str(item).encode("utf-8")).hexdigest())
        elif objective == "sleep":
            time.sleep(0.1)
            outputs.append(0.1)
        elif objective == "count":
            outputs.append(len(item) if hasattr(item, "__len__") else len(str(item)))
        else:
            raise ValueError("unknown objective: %r" % objective)
    elapsed_ms = int((time.monotonic() - start) * 1000)
    return outputs, elapsed_ms


def is_deadend(score, threshold=DEADEND_THRESHOLD):
    """Dead-end classification: score strictly below threshold."""
    return score < threshold


def now_rfc3339():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def build_result(task, worker_id, status, score, elapsed_ms, output, strategy_id):
    return {
        "taskId": task.get("taskId", ""),
        "jobId": task.get("jobId", ""),
        "workerId": worker_id,
        "status": status,
        "score": score,
        "metrics": {"elapsedMs": elapsed_ms},
        "output": output,
        "strategyId": strategy_id or task.get("strategyId", "gen-0"),
        "completedAt": now_rfc3339(),
    }


# ---------------------------------------------------------------------------
# Worker runtime (requires redis-py / nats-py)
# ---------------------------------------------------------------------------


class Worker:
    def __init__(self):
        import redis  # deferred so pure logic stays importable

        self.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        self.nc = None
        self.loop = None
        self.shutdown = False
        self.current_task = None  # in-flight task dict (for SIGTERM requeue)
        self.current_raw = None
        self.strategies = {}  # jobId -> StrategyJSON
        self._subscribed = set()  # jobIds with an active strategy subscription
        self.current_emitted = True  # True when no in-flight un-emitted task
        self.started_at = now_rfc3339()

    # -- NATS -------------------------------------------------------------
    def connect_nats(self):
        import asyncio
        import nats

        self.loop = asyncio.new_event_loop()
        try:
            self.nc = self.loop.run_until_complete(nats.connect(NATS_URL))
        except Exception as e:
            log.warning("NATS unavailable (%s); continuing without mesh", e)
            self.nc = None
            return
        # Run the event loop in a daemon thread so subscription callbacks
        # (strategy updates) dispatch promptly instead of only during
        # publishes.
        threading.Thread(target=self.loop.run_forever, daemon=True).start()

    def nats_publish(self, subject, payload):
        if self.nc is None:
            return
        import asyncio

        try:
            fut = asyncio.run_coroutine_threadsafe(
                self.nc.publish(subject, json.dumps(payload).encode("utf-8")),
                self.loop,
            )
            fut.result(timeout=5)
        except Exception as e:
            log.warning("nats publish %s failed: %s", subject, e)

    def subscribe_strategy(self, job_id):
        if self.nc is None or job_id in self._subscribed:
            return

        async def _sub():
            async def cb(msg):
                try:
                    strat = json.loads(msg.data.decode("utf-8"))
                    self.strategies[job_id] = strat
                    log.info(
                        "strategy update for %s: generation=%s reason=%s params=%s",
                        job_id,
                        strat.get("generation"),
                        strat.get("mutationReason"),
                        strat.get("params"),
                    )
                except Exception as e:
                    log.warning("bad strategy update: %s", e)

            await self.nc.subscribe("grid.strategy.update.%s" % job_id, cb=cb)

        import asyncio

        try:
            fut = asyncio.run_coroutine_threadsafe(_sub(), self.loop)
            fut.result(timeout=5)
            self._subscribed.add(job_id)
        except Exception as e:
            log.warning("strategy subscribe failed: %s", e)

    # -- heartbeat ----------------------------------------------------------
    def heartbeat_loop(self):
        node = {
            "nodeId": WORKER_ID,
            "role": "worker",
            "startedAt": self.started_at,
            "lastBeat": now_rfc3339(),
            "meta": {"gridId": GRID_ID, "jobId": JOB_ID},
        }
        while not self.shutdown:
            node["lastBeat"] = now_rfc3339()
            try:
                self.redis.hset("grid:state", WORKER_ID, json.dumps(node))
            except Exception as e:
                log.warning("heartbeat failed: %s", e)
            time.sleep(10)

    # -- task processing ----------------------------------------------------
    def process_task(self, task):
        job_id = task["jobId"]
        ph = param_hash(task.get("params") or {})
        counters = "grid:counters:%s" % job_id

        # dedup check (SPEC 3): skip and report status="duplicate"
        if task.get("params") and self.redis.sismember(
            "grid:deadends:%s" % job_id, ph
        ):
            log.info("task %s duplicate (paramHash in deadends)", task.get("taskId"))
            result = build_result(
                task, WORKER_ID, "duplicate", 0.0, 0, "duplicate",
                self._strategy_id(job_id, task),
            )
            self._emit(task, result)
            self.redis.hincrby(counters, "deadends", 1)
            # a duplicate is still a finished task
            self.redis.hincrby(counters, "completed", 1)
            return result

        strat = self.strategies.get(job_id)
        if strat:
            log.info(
                "applying strategy gen=%s to task %s",
                strat.get("generation"),
                task.get("taskId"),
            )

        start = time.monotonic()
        status, score, output = "ok", 0.0, ""
        try:
            if task.get("items"):
                objective = self._batch_objective(job_id)
                outputs, _ = run_batch_objective(objective, task["items"])
                score = float(len(task["items"]))
                output = json.dumps(outputs)
            else:
                score = hpo_score(task.get("params"))
                output = "hpo eval score=%.6f" % score
        except Exception as e:
            status, output = "failed", "error: %s" % e
        elapsed_ms = int((time.monotonic() - start) * 1000)

        if status == "ok" and is_deadend(score):
            status = "deadend"
            self.redis.sadd("grid:deadends:%s" % job_id, ph)
            self.redis.hincrby(counters, "deadends", 1)
            self.nats_publish(
                "grid.deadends.share",
                {"jobId": job_id, "paramHash": ph, "taskId": task.get("taskId")},
            )

        result = build_result(
            task, WORKER_ID, status, score, elapsed_ms, output,
            self._strategy_id(job_id, task),
        )
        self._emit(task, result)
        self.redis.hincrby(
            counters, "completed" if status == "ok" else "failed", 1
        )

        # conditional best update + breakthrough
        if status == "ok":
            best_key = "grid:best:%s" % job_id
            current = self.redis.get(best_key)
            current_score = None
            if current:
                try:
                    current_score = json.loads(current).get("score")
                except Exception:
                    current_score = None
            if current_score is None or score > current_score:
                self.redis.set(best_key, json.dumps(result))
                self.nats_publish("grid.results.breakthrough", result)
                log.info("new best for %s: %.6f", job_id, score)
        return result

    def _strategy_id(self, job_id, task):
        strat = self.strategies.get(job_id)
        if strat:
            return strat.get("strategyId", task.get("strategyId", "gen-0"))
        return task.get("strategyId", "gen-0")

    def _batch_objective(self, job_id):
        """Return the batch objective name for the job (default 'count')."""
        try:
            raw = self.redis.get("grid:job:%s" % job_id)
            if raw:
                job = json.loads(raw)
                return job.get("payload", {}).get("objective", "count")
        except Exception as e:
            log.warning("job lookup failed: %s", e)
        return "count"

    def _emit(self, task, result):
        job_id = task["jobId"]
        self.redis.xadd(
            "grid:telemetry:%s" % job_id,
            {
                "workerId": WORKER_ID,
                "taskId": str(task.get("taskId", "")),
                "jobId": job_id,
                "score": str(result["score"]),
                "elapsedMs": str(result["metrics"]["elapsedMs"]),
                "status": result["status"],
                "strategyId": result["strategyId"],
                # params let the strategy engine build a real GA population
                "params": json.dumps(task.get("params") or {}),
            },
        )
        self.redis.lpush("grid:results:%s" % job_id, json.dumps(result))
        self.redis.hdel("grid:processing:%s" % job_id, str(task.get("taskId")))
        self.current_emitted = True

    # -- main loop ----------------------------------------------------------
    def requeue_current(self):
        if self.current_raw is not None and self.current_task is not None:
            job_id = self.current_task["jobId"]
            log.warning(
                "requeueing in-flight task %s", self.current_task.get("taskId")
            )
            try:
                self.redis.rpush("grid:queue:%s" % job_id, self.current_raw)
                self.redis.hdel(
                    "grid:processing:%s" % job_id,
                    str(self.current_task.get("taskId")),
                )
            except Exception as e:
                log.error("requeue failed: %s", e)

    def handle_sigterm(self, signum, frame):
        log.info("SIGTERM received; shutting down gracefully")
        self.shutdown = True
        # Requeue only when the in-flight task was never emitted; otherwise
        # its result already reached the mesh and requeueing would duplicate it.
        if not self.current_emitted:
            self.requeue_current()
        sys.exit(0)

    def job_ids(self):
        if JOB_ID:
            return [JOB_ID]
        try:
            return list(self.redis.hkeys("grid:jobs")) or []
        except Exception as e:
            log.warning("cannot list jobs: %s", e)
            return []

    def run(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGINT, self.handle_sigterm)
        self.connect_nats()
        self.nats_publish(
            "grid.nodes.hello",
            {
                "nodeId": WORKER_ID,
                "role": "worker",
                "startedAt": self.started_at,
                "lastBeat": now_rfc3339(),
                "meta": {"gridId": GRID_ID},
            },
        )
        for jid in self.job_ids():
            self.subscribe_strategy(jid)
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        log.info("worker %s started (job=%s)", WORKER_ID, JOB_ID or "<all>")

        while not self.shutdown:
            ids = self.job_ids()
            if not ids:
                time.sleep(POLL_TIMEOUT)
                continue
            # pick up strategy subscriptions for jobs submitted after startup
            for jid in ids:
                self.subscribe_strategy(jid)
            keys = ["grid:queue:%s" % j for j in ids]
            try:
                item = self.redis.brpop(keys, timeout=POLL_TIMEOUT)
            except Exception as e:
                log.warning("BRPOP failed: %s; retrying", e)
                time.sleep(1)
                continue
            if item is None:
                continue
            _key, raw = item
            try:
                task = json.loads(raw)
            except Exception as e:
                log.warning("bad task payload: %s", e)
                continue
            self.current_task, self.current_raw = task, raw
            self.current_emitted = False
            # visibility move to grid:processing
            self.redis.hset(
                "grid:processing:%s" % task["jobId"], str(task.get("taskId")), raw
            )
            try:
                self.process_task(task)
            except Exception as e:
                log.exception("task %s failed: %s", task.get("taskId"), e)
                self.redis.hincrby(
                    "grid:counters:%s" % task["jobId"], "failed", 1
                )
                self.redis.hdel(
                    "grid:processing:%s" % task["jobId"], str(task.get("taskId"))
                )
                # failed tasks are terminal here; do not requeue on SIGTERM
                self.current_emitted = True
            finally:
                self.current_task, self.current_raw = None, None


def main():
    Worker().run()


if __name__ == "__main__":
    main()
