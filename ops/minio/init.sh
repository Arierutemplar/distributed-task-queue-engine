#!/bin/sh
set -eu

attempt=0
until mc alias set archive http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "MinIO initialization failed: destination is unavailable" >&2
    exit 1
  fi
  sleep 2
done

case "$AUDIT_WORM_LOCK_MODE" in
  GOVERNANCE) mode=governance ;;
  COMPLIANCE) mode=compliance ;;
  *) echo "Invalid Object Lock mode" >&2; exit 1 ;;
esac

if [ "$AUDIT_WORM_ACCESS_KEY_ID" = "$MINIO_ROOT_USER" ]; then
  echo "The audit writer must not be the MinIO root user" >&2
  exit 1
fi

mc mb --ignore-existing --with-lock "archive/$AUDIT_WORM_BUCKET"
mc retention set --default "$mode" "${AUDIT_WORM_RETENTION_DAYS}d" "archive/$AUDIT_WORM_BUCKET"
mc retention info --default "archive/$AUDIT_WORM_BUCKET"

cat > /tmp/audit-writer-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject", "s3:PutObjectRetention"],
    "Resource": ["arn:aws:s3:::$AUDIT_WORM_BUCKET/*"]
  }]
}
EOF
mc admin user add archive "$AUDIT_WORM_ACCESS_KEY_ID" "$AUDIT_WORM_SECRET_ACCESS_KEY" >/dev/null
mc admin policy create archive audit-worm-writer /tmp/audit-writer-policy.json >/dev/null
mc admin policy attach archive audit-worm-writer --user "$AUDIT_WORM_ACCESS_KEY_ID" >/dev/null
echo "MinIO Object Lock bucket and restricted audit writer initialized"
