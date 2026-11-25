#!/bin/sh
set -eu
attempt=0
until bao status >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "OpenBao initialization failed: server is unavailable or sealed" >&2
    exit 1
  fi
  sleep 1
done
bao token lookup >/dev/null
if bao kv metadata get secret/task-queue >/dev/null 2>&1; then
  echo "OpenBao demo KV v2 secret already exists; retaining its values"
  exit 0
fi

set -- "REDIS_PASSWORD=$REDIS_PASSWORD" "API_KEYS=$API_KEYS"
for key in ADMIN_PASSWORD OIDC_CLIENT_SECRET SESSION_SECRET AUDIT_WORM_ACCESS_KEY_ID AUDIT_WORM_SECRET_ACCESS_KEY REDIS_SENTINEL_PASSWORD; do
  value="$(printenv "$key")"
  if [ -n "$value" ]; then
    set -- "$@" "$key=$value"
  fi
done
bao kv put secret/task-queue "$@" >/dev/null
echo "OpenBao demo KV v2 secret initialized (values are not logged)"
