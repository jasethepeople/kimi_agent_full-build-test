#!/bin/sh
# Thin wrapper: forward to the worker (exec so signals reach python).
set -e
exec python /app/worker.py "$@"
