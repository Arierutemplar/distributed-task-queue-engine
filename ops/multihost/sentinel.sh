#!/bin/sh
set -eu
umask 077

if [ ! -f /data/sentinel.conf ]; then
  cat > /data/sentinel.conf <<EOF
port 26379
dir /data
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip $SENTINEL_ANNOUNCE_IP
sentinel monitor $REDIS_SENTINEL_MASTER_NAME $REDIS_HA_PRIMARY_IP 6379 2
sentinel auth-pass $REDIS_SENTINEL_MASTER_NAME $REDIS_PASSWORD
sentinel down-after-milliseconds $REDIS_SENTINEL_MASTER_NAME 5000
sentinel failover-timeout $REDIS_SENTINEL_MASTER_NAME 15000
sentinel parallel-syncs $REDIS_SENTINEL_MASTER_NAME 1
EOF
fi

exec /usr/local/bin/docker-entrypoint.sh redis-server /data/sentinel.conf --sentinel
