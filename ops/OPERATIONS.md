# Operational reliability contract and runbook

## Objectives, not guarantees

| Indicator                | Objective                         | Measurement                                                                                                                                               |
| ------------------------ | --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Monthly API availability | 99.9% (43m12s budget per 30 days) | External authenticated HTTPS synthetic probe and request success at the proxy; retain a full month                                                        |
| API acceptance           | p95 <= 500ms                      | Valid document POST returning 202, measured by the open-loop driver; Prometheus histogram is an approximation                                             |
| Document completion      | p95 <= 5s                         | Creation through terminal successful completion, including queue/retries; driver includes acceptance and up to 50ms polling delay                         |
| Recovery point (RPO)     | <= 15min                          | Age of capture timestamp in the last **downloaded, decrypted and RDB-verified** off-host snapshot                                                         |
| Recovery time (RTO)      | <= 15min                          | Incident detection through restored service and TLS proxy cutover; the automated drill measures only **technical restore**, not the whole operational RTO |

The reproducible reference workload is 20 document submissions/second for 30 seconds,
followed by a 100/second 5-second pulse, 1,100 ASCII bytes/document, at most 128 driver
requests in flight, and worker concurrency 10. It verifies every result SHA256 and fails
on any unexpected HTTP response, missed task, driver overload or latency breach.
Status/result reads are eventually consistent; a completed state with a temporarily
null result is polled again within the same deadline, never counted as verified completion.
Record CPU/RAM, image digest, Redis size and host contention with each capacity test.
This small workload is not a production capacity certification. Repeat at the expected
production dataset, payload distribution and concurrency. Successful completions alone
do not describe failure rate: monitor DLQ, queue wait/backlog and failed attempts too.

Compose runs on one host: Sentinel improves process/container failover, **not host HA**.
An external TLS proxy, managed identity provider, off-host Object Lock storage, separately
protected encryption keys and external monitoring/on-call are production prerequisites.
The local Keycloak/OpenBao/MinIO profiles are development fixtures, not production services.
Prometheus scrape health is not an external availability measurement. Configure an
external authenticated probe, alert delivery/Alertmanager, ownership and escalation.

## Delivery and retention

- Set `QUEUE_MAX_WAITING` and `TENANT_MAX_INFLIGHT` consistently on all API
  replicas (0 disables each). The tenant quota covers waiting, active, delayed
  retries and waiting-children; queue depth covers waiting, paused and prioritized.
  Capacity rejections return 429 with `Retry-After: 1`. Monitor
  `task_backpressure_rejections_total` and `tenant_tasks_inflight`. Every admission
  with the tenant limit enabled, and every metrics scrape, scans the inflight queue atomically
  in Redis; benchmark representative backlog and payload sizes before enabling.
  FIFO and bounded inflight prevent an endlessly growing single-tenant backlog,
  but do not reserve worker slots or provide strict round-robin scheduling.
  Bull Board/manual Redis writes and old API replicas can bypass admission limits.
  Tenant IDs appear as metric labels: restrict metrics access and budget cardinality.
- For API-key rotation, add the new key under the same tenant in Vault/OpenBao or
  an atomically replaced, access-controlled `API_KEYS_FILE`, refresh every API replica
  via `POST /admin/api/secrets/reload` or wait for its configured
  `SECRETS_REFRESH_INTERVAL_MS`, migrate clients, then remove the old key and refresh
  again. Verify old and new credentials on every replica. Empty keys after secured
  startup deny all callers. Refresh failures retain previous keys and must be alerted;
  Vault missing fields fall back to startup environment values. Admin/Redis/OIDC/S3
  credentials are not hot-swapped and require a controlled restart.
- Task payloads may omit `schemaVersion` (equivalent to `1`) or explicitly use `1`.
  Deploy consumers that support a new version before allowing producers to send it;
  unsupported versions are rejected by the API or fail without retry if inserted directly.
- Task delivery is **at least once**, not exactly once. A 202 means Redis accepted the
  job; asynchronous replication and `appendfsync everysec` can lose recent acknowledged
  writes. Sentinel does not provide zero RPO. RPO depends on off-host backups remaining fresh.
- Optional idempotency survives only while its original job is retained. External
  billing/notification handlers must deduplicate business effects with durable business IDs.
  Current billing/notification integrations remain demonstrative.
- `DLQ_RECONCILE_ENABLED=true` scans retained terminal failures in bounded batches and
  uses the existing atomic generation-aware routing script. It repairs a lost failed-event
  callback, avoids duplicates, and respects operator deletion of an already routed entry.
  Pass success is not proof that a complete large failed set was scanned; size batches
  and intervals to the actual backlog and monitor recovery time.
- In this mode terminal originals are **not automatically deleted**, otherwise a missed
  callback could become irrecoverable. Completed-job retention remains unchanged. DLQ and
  failed audit-outbox jobs also remain until handled. Monitor Redis capacity/noeviction.
- `AUDIT_WORM_DELIVERY=outbox` persists the sanitized audit event and stable `archiveId`
  in a Redis BullMQ queue. Delivery retries and maintenance revisits exhausted jobs after
  storage recovery, including after process restart. Credentials/errors are redacted.
- Recovery is guaranteed only **after successful outbox enqueue**, within Redis durability
  and the RPO contract. An admin mutation and its audit enqueue are **not atomic**:
  process crashes/Redis outages can still leave an audit gap. `enqueue_failed` is a critical
  alert; reconcile structured logs and the Redis audit stream manually. This is not a
  transactionally complete regulated ledger.
- Archival is at least once: an ambiguous storage ACK may create multiple immutable
  versions of the same key/body. Consumers deduplicate by `archiveId`, not object count.
  The backlog age metric samples the head of each state, not all creation timestamps.
- Default mode stays Basic + standalone Redis + direct/disabled WORM, with the original
  bounded task failure retention and no backup service.

### Managed failed-original cleanup

Weekly, review failed originals older than seven days. During a maintenance window,
block admin writes at the proxy, pause task intake, stop/drain task workers and obtain a
verified backup. Confirm **each candidate** has a generation marker `dtqDlqRouted` matching
`finishedOn:attemptsMade` (and its DLQ entry or documented operator deletion). Delete only
those confirmed originals through BullMQ `Job.remove()`, in batches <= 100. Do not use
`FLUSHDB`, blanket `Queue.clean('failed')`, or delete unmarked failures. Resume workers and
intake, verify reconciliation freshness/readiness and compare counts. Original deletion
also expires task idempotency. Apply a separately approved policy to retained DLQ entries.
Never delete failed audit jobs to make a dashboard green; repair/recover the archive.

## Off-host backups

Provision a dedicated **versioned Object Lock** bucket in a different host/failure domain,
with COMPLIANCE retention >= 90 days. Example for AWS us-east-1 (use `--endpoint-url`
for an externally managed MinIO/S3 service):

```sh
aws s3api create-bucket --bucket YOUR-BACKUP-BUCKET --object-lock-enabled-for-bucket --region us-east-1
aws s3api put-object-lock-configuration --bucket YOUR-BACKUP-BUCKET \
  --object-lock-configuration '{"ObjectLockEnabled":"Enabled","Rule":{"DefaultRetention":{"Mode":"COMPLIANCE","Days":90}}}'
```

Use a dedicated backup identity allowing `s3:PutObject`, `s3:GetObject`,
`s3:PutObjectRetention`, optionally `s3:GetObjectRetention`/`s3:ListBucket` for operators.
Deny delete/version-delete, governance bypass and changes to versioning/Object Lock.
For audit use a separate write-only identity and bucket. Confirm permissions and retention
against the **real** destination; the reliability HTTP fixture is not an Object Lock proof.
COMPLIANCE does not prevent destruction of the entire storage host, account or encryption key.

The agent obtains a consistent Redis replication RDB, checks it with Redis 7
`redis-check-rdb`, streams AES-256-GCM encryption, uploads with SHA256 and COMPLIANCE
retention, downloads, authenticates and checks the RDB again before declaring success.
Capture time is authenticated; the verified plaintext SHA256 must match the source.
Uploads report their immutable `versionId`, and remote verification fetches that exact
version. Supply both key and version to subsequent restore operations, not just latest.
Snapshot size and execution are bounded; default interval
5min + maximum capture budget 4min fits the 15min RPO. Failures mark freshness/metrics,
not a successful backup. Snapshots include **all databases in the Redis instance**, so
use a dedicated instance and appropriate replication/CLIENT INFO backup ACL privileges.
Private temporary plaintext is removed after the operation; reserve >= 3x the maximum
snapshot size in temporary storage (default 3GiB). Increase tmpfs and host capacity when
increasing `BACKUP_MAX_BYTES`. Monitor disk/memory, snapshot duration and archive bandwidth.

Set variables in the deployment environment/secret manager, **not the local `.env`**:
`BACKUP_S3_ENDPOINT`, `BACKUP_S3_BUCKET`, credentials and `BACKUP_ENCRYPTION_KEY`
(64 hex characters). Vault supports the backup keys; use the dedicated
`BACKUP_SECRETS_PROVIDER` and `BACKUP_VAULT_*` identity/path, not the app secret.
Retain the key independently of Redis and the backup bucket. Rotate by changing
`BACKUP_KEY_ID` and key together; retain old key IDs/keys until all corresponding
immutable objects expire. Verify a restore with each retained key generation.

PowerShell commands (same file/profile flags work in POSIX shells):

```powershell
# Generate once and deposit securely in the deployment secret manager; never print/commit it.
$env:BACKUP_ENCRYPTION_KEY = node -e "process.stdout.write(require('node:crypto').randomBytes(32).toString('hex'))"
$env:BACKUP_S3_ENDPOINT = 'https://storage.example.com'
$env:BACKUP_S3_BUCKET = 'YOUR-BACKUP-BUCKET'
# Supply BACKUP_S3_ACCESS_KEY_ID and BACKUP_S3_SECRET_ACCESS_KEY securely, or use Vault.
docker compose -f docker-compose.yml -f docker-compose.operations.yml --profile operations up -d --build
docker compose -f docker-compose.yml -f docker-compose.operations.yml --profile operations exec -T backup node dist/src/operations/backup-cli.js once
# Copy the exact key from backup-report, using the correct key generation.
$key = 'distributed-task-queue/backups/v1/YYYY-MM-DD/UUID.rdb.enc'
$version = 'VERSION-ID-FROM-BACKUP-REPORT'
docker compose -f docker-compose.yml -f docker-compose.operations.yml --profile operations exec -T backup node dist/src/operations/backup-cli.js restore-check $key $version
```

For Sentinel also include `-f docker-compose.ha.yml --profile ha-redis` and set the
Sentinel selectors as in the existing HA runbook. The agent discovers the current
master, not a hardcoded original-primary IP.

`restore-check` always uses a **new isolated loopback Redis**, no AOF, and a separate
probe queue executing the real document handler. It reports restored task/outbox counts,
RPO and technical restore time. It never runs restored invoices/notifications.

### Real disaster recovery

1. Start the incident timer, stop intake/admin mutations at the TLS proxy and fence the old
   writer/Redis primary to avoid split-brain. Record detection and decision time.
2. Identify a known-good object **version**, capture time and matching encryption-key
   generation. Check its age against RPO; do not silently select a stale snapshot.
   Preserve version IDs from the S3 inventory; immutable versions survive replacement of
   a latest key. Restrict writers and investigate any unexpected version.
3. Run `restore-check` from the operations image on the recovery host. The isolated
   directory is removed afterwards; this command verifies recovery, not production cutover.
4. Securely materialize the same authenticated RDB into a **new** Redis data volume using
   `restore-export` (existing, empty absolute destination; exclusive `dump.rdb` creation).
   It rejects stale snapshots beyond the configured RPO instead of silently loading them.
   Example, only after fencing the old writer:

   ```powershell
   docker volume create dtq_recovery_YYYYMMDD
   docker run --rm --user 0 -v dtq_recovery_YYYYMMDD:/restore --entrypoint sh $env:OPERATIONS_IMAGE -c 'chown 1000:1000 /restore'
   docker compose -f docker-compose.yml -f docker-compose.operations.yml --profile operations run --rm --no-deps -v dtq_recovery_YYYYMMDD:/restore backup node dist/src/operations/backup-cli.js restore-export $key /restore $version
   docker run --rm -v dtq_recovery_YYYYMMDD:/data redis:7-alpine redis-check-rdb /data/dump.rdb
   ```

   Check with
   `redis-check-rdb` and start Redis 7 with AOF **disabled** for the first load. An old AOF
   overrides an RDB; never overwrite a live volume or reuse an old AOF directory.

5. Verify Redis key/job counts and business samples against the drill report. Enable AOF
   after the RDB has loaded and verify persistence before admitting writes. For Sentinel,
   bootstrap a fresh replica/Sentinel topology against the restored primary; do not reuse
   stale Sentinel master state without review.
6. Start API and reconciliation with task workers/intake initially held. Review active
   jobs, retries and side-effect deduplication before resuming workers. Audit outbox
   resumes archived delivery by stable IDs; duplicates are possible.
7. Check authenticated POST/status, OIDC viewer/admin, backlog, TLS routing and external
   synthetic probe, then cut over the proxy. Record **full** RTO including human/proxy steps.
8. Compare both objectives and data loss with captured acceptance records. Preserve
   incident evidence and review failures; do not call a technical drill a full RTO proof.

## Automated validation

```powershell
npm run ci
npm run ops:reliability
```

Fast CI uses mocks plus a disposable real Redis (the existing CI Redis setup), not
Keycloak, Vault or S3. The separate scheduled/manual reliability workflow builds an
isolated project, publishes **no host ports**, uses ephemeral fixture credentials and a
non-overlapping Docker subnet, and removes only its own containers/volumes/images.
It tests sustained/pulse load, archive outage plus restart recovery, lost DLQ callback,
actual primary-container stop/Sentinel promotion, prior-task preservation, new-task
completion, and encrypted backup/restore **including pending audit state**.
Set `RELIABILITY_REPORT` to save the JSON report and `RELIABILITY_LOAD_SECONDS` to
increase the sustained run (5..300). Logs/errors are not a substitute for passed gates.

Run a real off-host restore drill monthly and after Redis/image/storage/key changes.
Inject a representative dataset large enough to challenge RPO/RTO. Test a complete
host outage, detection and proxy cutover separately; the single-host fixture cannot.

## Production Compose and rollout/rollback

Use Compose >= 2.24.4, external HTTPS OIDC/proxy/storage and securely supplied credentials.
Use approved immutable `APP_IMAGE` and `OPERATIONS_IMAGE` **digest references**, not `latest`.
Set `AUDIT_WORM_ENABLED=true`, HTTPS archival URL/writer identity, API keys, explicit HTTPS
origins/OIDC URLs, session secret and off-host backup settings before deployment.
Production config rejects missing/unsafe settings; `NODE_ENV=production` alone retains
legacy compatibility and does not activate these extra checks.

```powershell
docker compose -f docker-compose.yml -f docker-compose.operations.yml -f docker-compose.production.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.operations.yml -f docker-compose.production.yml pull
docker compose -f docker-compose.yml -f docker-compose.operations.yml -f docker-compose.production.yml up -d --no-build --wait --wait-timeout 300
# Optional local monitoring; configure external alert delivery separately.
docker compose -f docker-compose.yml -f docker-compose.operations.yml -f docker-compose.production.yml --profile observability up -d --no-build
```

The overlay separates API/worker roles, removes Redis published ports, binds the API
only to host loopback, sets read-only application filesystems, private tmpfs, dropped
capabilities/no-new-privileges and resource/log budgets. Redis is private/authenticated;
plain Redis transport is appropriate only on this private same-host network. External
Redis requires `rediss://`. The TLS proxy must be on the same host, overwrite forwarded
headers, block direct bypass and forward exactly one trusted hop. Do not blindly increase
`TRUST_PROXY`. Redis has noeviction and explicit memory headroom. Tune budgets to data size.

Monitoring config/recording/alert rules are provided, **not alert delivery**. Verify
Prometheus targets and wire an external on-call receiver. Alert on failed audit enqueue,
backlog, stale maintenance/backups, API errors/latency and DLQ. Configure storage health,
Redis memory/AOF/replication and external synthetic monitoring independently.

Rollout:

1. Record current image digests/config and a verified recovery object/key generation.
   Review BullMQ key/schema and business-result compatibility; no schema migration is
   performed automatically. Test new and old images against restored state beforehand.
2. At the proxy stop task intake and admin writes; drain workers using SIGTERM and the
   existing graceful-shutdown budget (longer than allowed jobs). Verify no active jobs.
3. Deploy the new digest pair using `up -d --no-build --wait`. Do not remove Redis volumes.
   Run authenticated task/status and OIDC admin/viewer checks through the HTTPS proxy.
4. Resume traffic, observe SLO/backlog/backup alerts through the agreed canary window.
   Compose replacement is not automatically zero-downtime; account for it in availability.

Rollback:

1. Quiesce traffic again, drain the new workers and restore the previous approved image
   digest pair/config, then `up -d --no-build --wait`. Verify task/admin probes and resume.
2. Never run old processors against an incompatible job schema. If incompatible, fence
   traffic and execute reviewed DR/migration; restoring data can lose acknowledged writes
   within RPO. Never `down -v` production or launch a second unfenced writer.
3. Record actual outage duration and evidence; a successful readiness probe alone is
   neither a completed rollback nor proof of preserved task/audit data.
