#!/usr/bin/env bash
# hpo-grid end-to-end smoke test.
# Waits for the orchestrator health endpoint, submits scripts/seed_job.json,
# then polls the job until at least EXPECTED_COMPLETED tasks complete or the
# timeout elapses. Prints the best result on success; exits non-zero on failure.
set -euo pipefail

API="${API:-http://localhost:8080}"
TIMEOUT_SEC="${TIMEOUT_SEC:-120}"
EXPECTED_COMPLETED="${EXPECTED_COMPLETED:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SEED_FILE="${SEED_FILE:-${SCRIPT_DIR}/seed_job.json}"

log() { printf '[smoke] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || fail "required tool not found: $1"; }
need curl

extract() { # extract <dotted.path> from stdin JSON
  if command -v jq >/dev/null 2>&1; then
    jq -r "$1 // empty"
  else
    python3 -c "import json,sys
expr = sys.argv[1].lstrip('.')
data = json.load(sys.stdin)
for part in expr.split('.'):
    if not isinstance(data, dict):
        data = None
        break
    data = data.get(part)
print('' if data is None else data)"
  fi
}

log "waiting for orchestrator at ${API}/healthz ..."
deadline=$(( $(date +%s) + TIMEOUT_SEC ))
until curl -fsS "${API}/healthz" >/dev/null 2>&1; do
  [ "$(date +%s)" -lt "$deadline" ] || fail "orchestrator not healthy within ${TIMEOUT_SEC}s"
  sleep 2
done
log "orchestrator is healthy"

log "submitting seed job ${SEED_FILE}"
resp=$(curl -fsS -X POST "${API}/api/v1/jobs" \
  -H 'Content-Type: application/json' \
  --data @"${SEED_FILE}") || fail "job submission failed"
job_id=$(printf '%s' "$resp" | extract '.jobId')
[ -n "$job_id" ] || fail "could not parse jobId from response: ${resp}"
log "job submitted: ${job_id}"

log "polling until counters.completed >= ${EXPECTED_COMPLETED} (timeout ${TIMEOUT_SEC}s)"
deadline=$(( $(date +%s) + TIMEOUT_SEC ))
while true; do
  body=$(curl -fsS "${API}/api/v1/jobs/${job_id}" 2>/dev/null || true)
  completed=$(printf '%s' "$body" | extract '.counters.completed')
  completed=${completed:-0}
  status=$(printf '%s' "$body" | extract '.job.status')
  [ -n "$status" ] || status=$(printf '%s' "$body" | extract '.status')
  log "job=${job_id} status=${status:-unknown} completed=${completed}"
  if [ "$completed" -ge "$EXPECTED_COMPLETED" ]; then
    break
  fi
  [ "$(date +%s)" -lt "$deadline" ] || fail "timeout: only ${completed} task(s) completed"
  sleep 3
done

log "completed threshold reached; best result:"
best=$(printf '%s' "$body" | extract '.best')
if [ -n "$best" ]; then
  printf '%s\n' "$best" | (command -v jq >/dev/null 2>&1 && jq . || python3 -m json.tool) 2>/dev/null || printf '%s\n' "$best"
else
  log "(no best recorded yet)"
fi
log "PASS"
