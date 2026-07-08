#!/usr/bin/env bash
# Polls heyvmd's local API and force-restarts it via supervisorctl after
# consecutive failures. Runs as its own supervisor program (see heyvmd.conf)
# because supervisor's autorestart only catches a heyvmd process that has
# actually exited, not one that's alive but no longer answering requests.
set -u

PORT="${HEYVMD_HEALTHCHECK_PORT:-34099}"
URL="http://127.0.0.1:${PORT}/deployed-sandboxes"
INTERVAL="${HEYVMD_HEALTHCHECK_INTERVAL_SECS:-15}"
FAIL_THRESHOLD="${HEYVMD_HEALTHCHECK_FAIL_THRESHOLD:-3}"
COOLDOWN_SECS="${HEYVMD_HEALTHCHECK_COOLDOWN_SECS:-60}"

fails=0
while true; do
  if curl -fsS --max-time 5 "$URL" >/dev/null 2>&1; then
    fails=0
  else
    fails=$((fails + 1))
    echo "$(date -Is) heyvmd health check failed (${fails}/${FAIL_THRESHOLD}): $URL"
    if [ "$fails" -ge "$FAIL_THRESHOLD" ]; then
      echo "$(date -Is) restarting heyvmd after ${fails} consecutive failures"
      supervisorctl restart heyvmd
      fails=0
      sleep "$COOLDOWN_SECS"
    fi
  fi
  sleep "$INTERVAL"
done
