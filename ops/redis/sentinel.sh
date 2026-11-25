#!/bin/sh
set -eu
umask 077

if [ ! -f /data/sentinel.conf ]; then
  cat > /data/sentinel.conf <<EOF
port 26379
dir /data
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel monitor $REDIS_SENTINEL_MASTER_NAME ${REDIS_HA_PRIMARY_IP:-redis} 6379 2
sentinel auth-pass $REDIS_SENTINEL_MASTER_NAME $REDIS_PASSWORD
sentinel down-after-milliseconds $REDIS_SENTINEL_MASTER_NAME 5000
sentinel failover-timeout $REDIS_SENTINEL_MASTER_NAME 15000
sentinel parallel-syncs $REDIS_SENTINEL_MASTER_NAME 1
EOF
  if [ -n "$REDIS_SENTINEL_PASSWORD" ]; then
    printf 'requirepass %s\n' "$REDIS_SENTINEL_PASSWORD" >> /data/sentinel.conf
    printf 'sentinel sentinel-pass %s\n' "$REDIS_SENTINEL_PASSWORD" >> /data/sentinel.conf
  fi
fi

exec /usr/local/bin/docker-entrypoint.sh redis-server /data/sentinel.conf --sentinel
