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

## Alert ownership and response

The production Prometheus overlay loads `ops/prometheus/operational-rules.yml`.
Each alert has `severity`, `team`, `owner` and `runbook_url`. `platform-oncall`
owns API, queue, circuit, DLQ, maintenance and backup alerts; `security-oncall`
owns audit delivery alerts. These are **role identifiers**, not configured
people or paging destinations: before production, assign real primary/secondary
on-call staff and escalation contacts, route critical and warning alerts to
appropriate Alertmanager receivers, and test delivery and acknowledgement.
The default observability profile does not load these production rules or provide
an Alertmanager receiver. Thresholds (including 100 queued jobs) are starting
points and must be tuned against measured workload and capacity.

Validate rules and representative firing cases with the Prometheus image:

```powershell
$mount = (Join-Path (Get-Location).Path 'ops\prometheus') + ':/etc/prometheus:ro'
docker run --rm --entrypoint promtool -v $mount prom/prometheus check rules /etc/prometheus/operational-rules.yml
docker run --rm --entrypoint promtool -v $mount prom/prometheus test rules /etc/prometheus/operational-rules.test.yml
```

### Alert: API errors

**Owner: platform-oncall; critical.** Check the external probe and TLS proxy, then
API readiness, Redis connectivity and 5xx logs by request ID. Escalate an outage,
restore the failing dependency and verify authenticated submissions. `up` tests
scrapes, not end-user availability; a no-traffic error ratio has no samples.

### Alert: queue saturation

**Owner: platform-oncall; warning.** Check waiting/paused/prioritized counts,
admission rejections, worker health, throughput, retries and Redis capacity.
Mitigate load or restore workers before changing quotas; confirm backlog drains
and acceptance/completion latency recovers. Queue gauges repeat across replicas;
the rule takes the maximum per-instance total rather than summing copies.

### Alert: circuit and DLQ

**Owner: platform-oncall; warning for circuit/DLQ, critical for stale maintenance.**
Inspect billing dependency and worker logs, reconcile ambiguous external effects
using their original idempotency keys, then replay only reviewed DLQ jobs. Verify
maintenance is running and backlog drains. Circuit metrics are each worker's
last observed state, so confirm the shared circuit before acting.

### Alert: audit delivery

**Owner: security-oncall; critical for enqueue failure, warning for backlog.**
Check archival credentials, storage and outbox worker; restore delivery without
deleting pending jobs. For enqueue failures reconcile application logs and the
Redis audit stream, since the archive may have a gap. Escalate to platform-oncall
for infrastructure failures and verify archived records after recovery.

### Alert: backup

**Owner: platform-oncall; critical for stale/unavailable backup, warning for failed
attempts.** Check agent health, destination access, encryption keys, snapshot
size, bandwidth and the timestamp of the last remotely verified version. Restore
the latest known-good version in isolation; escalate before the RPO expires.
Freshness alone is not proof that a production restore or off-host storage works.

## External monitoring probe

Run the probe from a separate monitoring machine/network, against the externally
reachable TLS proxy. It checks `/health/ready` and `/metrics`, emits JSON with
status and per-request latency, and exits nonzero on failures. Configure external
monitoring to schedule it, retain results and page its own independent receiver.
The probe does not submit a task and is not an authenticated end-to-end availability
measurement. Restrict metrics access at the proxy; if it requires a bearer token,
set `EXTERNAL_PROBE_BEARER_TOKEN` securely in the probe environment. Requests time
out after 5 seconds by default; `EXTERNAL_PROBE_TIMEOUT_MS` accepts 1–60000.

For the default local stack, run from the host, outside the Compose network:

```powershell
node ops\reliability\external-probe.js http://127.0.0.1:3000
```

For real external monitoring replace the URL with `https://your-proxy.example`.
HTTP is accepted only for loopback development. A host-local success does not
survive host failure or establish independent monitoring.

## Simulated host loss

The standalone `docker-compose.multihost.yml` is an isolated, single-engine
failure-domain fixture. `host-a` and `host-b` each contain Redis, Sentinel, API and
worker; `witness` contains the third Sentinel. Quorum is two. A third independent
voting domain is necessary for majority after losing either of the two application
domains. Domain labels drive the exercise; the shared bridge is not a physical
network or machine boundary.

From the repository root, run:

```powershell
node ops\multihost\drill.js both
# Or select one domain:
node ops\multihost\drill.js host-a
node ops\multihost\drill.js host-b
```

The command builds and starts its own uniquely named project and image, chooses a
non-overlapping subnet, and uses SIGKILL on every service in the selected domain.
It checks surviving Sentinel quorum and primary, API/worker readiness, task
completion and hashes, and restores Redis and Sentinel before allowing the
returning API/worker to start. A Redis `WAIT` barrier confirms replica receipt of
preceding writes before failure. `both` fails A first, then B, so both primary
promotions are exercised. `host-b` alone starts with B as the replica and tests
its loss without a promotion. The JSON report records each scenario. Normal
success or failure removes only the fixture's project, volumes and image; if
the process is interrupted externally, inspect `docker compose ls --all` and
clean up only the identified fixture.

To launch a separate manual fixture without changing `.env` or the default stack,
use a fresh PowerShell session and a unique project/image:

```powershell
$project = "dtq-mh-manual-$([guid]::NewGuid().ToString('N').Substring(0, 12))"
$env:DTQ_MH_IMAGE = "${project}:local"
$env:DTQ_MH_REDIS_PASSWORD = [guid]::NewGuid().ToString('N')
$env:DTQ_MH_API_KEY = "fixture-$([guid]::NewGuid().ToString('N'))"
docker compose -p $project -f docker-compose.multihost.yml build api-a
docker compose -p $project -f docker-compose.multihost.yml up -d --no-build --wait
docker compose -p $project -f docker-compose.multihost.yml ps
# The automated exercise above creates its own fixture; it does not kill this one.
# When finished, delete only this disposable project's volumes and image:
docker compose -p $project -f docker-compose.multihost.yml down -v --remove-orphans
docker image rm $env:DTQ_MH_IMAGE
```

The manual fixture defaults to subnet `172.31.251.0/24`. If it overlaps an existing
network, set `DTQ_MH_SUBNET`, `DTQ_MH_REDIS_A_IP`, `DTQ_MH_REDIS_B_IP` and
`DTQ_MH_SENTINEL_A_IP`, `DTQ_MH_SENTINEL_B_IP`, `DTQ_MH_SENTINEL_W_IP` to
distinct addresses in a free subnet before starting. Its APIs are reachable only
inside the Compose network.

This verifies container-level behavior and sampled state, not actual host HA,
exhaustive state integrity, network partitions, loss of a disk/engine, or zero
RPO. Redis replication is asynchronous; `WAIT` acknowledges replica receipt, not
durable persistence. On a real system deploy the two data/app domains and the
witness in independent physical/VM failure domains, provide secure routing and
client-reachable advertised addresses, test fencing and partition recovery, and
use independent backups. Do not rely on this fixture as a production deployment.

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
- HTTP admission deduplication and 409 conflict detection survive only while the
  original BullMQ job is retained. Invoice effects have separate Redis records
  (`in_progress`/`completed`, fingerprint and result), retained for
  `EFFECT_RETENTION_SECONDS` (default 30 days, range 3600–31536000). The local
  payment simulator stores its result and `executions=1` separately with the same
  configured TTL from application time. A completed effect record's TTL starts
  again on completion; an in-progress record's TTL starts on the first attempt.
  Keep the service name, tenant identity and configuration consistent on replicas;
  size retention for the maximum retry, DLQ and recovery window, monitor Redis
  capacity, and retain provider keys at least as long.
- Reuse the same tenant and `Idempotency-Key` for uncertain HTTP retries. A worker
  retry or DLQ replay propagates the same provider key; independent submissions
  without that header do not deduplicate. An `in_progress` record does not lock a
  provider call. If the call committed but the response or Redis completion was
  lost, retry/reconcile with the same key. Real providers must atomically persist
  key, payload identity and result independently; if they do not, duplicate
  charges or notifications remain possible. The local payment simulator shares
  Redis with the queue and is not evidence against primary/replica data loss.
  Notification processing currently does not send an external notification.
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
Optionally set `RESTORE_SAMPLE_MANIFEST` on that command to a JSON manifest captured
independently before the snapshot by `captureRestoreSamples`; restore fails if any
sampled job state/content/result/failure or audit stream entry differs. The manifest
contains identifiers and hashes, not task payloads. Protect and retain it separately
from the backup, and select samples before capture; matching samples do not prove all
keys are intact.

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
The restore phase stops its own fixture app, seeds completed, failed, waiting and
delayed tasks, a DLQ entry, pending audit outbox and an audit stream record. It
captures a manifest before backup, uploads the encrypted snapshot to the fixture
S3 service's separate volume, downloads it and compares all seven samples in a
fresh isolated Redis. It also checks an exported RDB. Run the entire representative
restore scenario with:

```powershell
npm run ops:reliability
```

The weekly scheduled workflow runs this same exercise. Its fixture S3 volume is
separate from active Redis data but still on the same machine; it is **not off-host
storage**, does not prove Object Lock or survival of host loss, and is deleted with
the fixture. Use the real off-host bucket procedure above for production evidence.
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
