import process from 'node:process';
import { spawnSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
const project = `dtq-drill-${randomUUID().slice(0, 8)}`;
const env = {
  ...process.env,
  APP_IMAGE: `${project}:app`,
  OPERATIONS_IMAGE: `${project}:ops`,
  REDIS_PASSWORD: randomBytes(24).toString('hex'),
  API_KEYS: 'fixture-key-0123456789abcdef0123456789',
  ADMIN_USERNAME: 'admin',
  ADMIN_PASSWORD: randomBytes(24).toString('hex'),
  SERVICE_NAME: project,
  BACKUP_ENCRYPTION_KEY: randomBytes(32).toString('hex'),
  REDIS_SENTINEL_HOSTS: 'redis-sentinel-1:26379,redis-sentinel-2:26379,redis-sentinel-3:26379',
  REDIS_SENTINEL_MASTER_NAME: 'task-queue',
  REDIS_SENTINEL_PASSWORD: '',
  SECRETS_PROVIDER: 'env',
  SECRETS_REFRESH_INTERVAL_MS: '0',
  API_KEYS_FILE: '',
  QUEUE_MAX_WAITING: '0',
  TENANT_MAX_INFLIGHT: '0',
  TENANT_MAX_ACTIVE_SLOTS: '0',
  ADMIN_AUTH_MODE: 'basic',
  BILLING_FAILURE_RATE: '0',
  AUDIT_WORM_ENABLED: 'false',
  PRODUCTION_HARDENING_ENABLED: 'false',
};
const docker = (args) => {
  const result = spawnSync('docker', args, {
    env,
    encoding: 'utf8',
    maxBuffer: 8 * 1024 * 1024,
    timeout: 600000,
  });
  if (result.error || result.status !== 0)
    throw new Error(
      `Docker command failed (exit ${result.status}): ${result.error?.message || `${result.stderr.slice(-4000)}\n${result.stdout.slice(-8000)}`}`,
    );
  return result.stdout;
};
const networkIds = docker(['network', 'ls', '-q']).trim().split(/\s+/u).filter(Boolean);
const networks = networkIds.length ? JSON.parse(docker(['network', 'inspect', ...networkIds])) : [];
const range = (cidr) => {
  const [address, bits] = cidr.split('/');
  const ip = address.split('.').reduce((value, part) => value * 256 + Number(part), 0);
  const start = (ip & (0xffffffff << (32 - Number(bits)))) >>> 0;
  return [start, start + 2 ** (32 - Number(bits)) - 1];
};
const occupied = networks
  .flatMap((network) => network.IPAM.Config || [])
  .map((config) => config.Subnet)
  .filter((subnet) => subnet && !subnet.includes(':'))
  .map(range);
const subnet = Array.from({ length: 7 }, (_, index) => `172.${25 + index}.254.0/24`).find(
  (candidate) => {
    const [start, end] = range(candidate);
    return occupied.every(([first, last]) => end < first || start > last);
  },
);
if (!subnet) throw new Error('No free fixture subnet; no resources created');
env.REDIS_HA_SUBNET = subnet;
env.REDIS_HA_PRIMARY_IP = subnet.replace('0/24', '10');
env.REDIS_HA_REPLICA_IP = subnet.replace('0/24', '11');
const prefix = [
  'compose',
  '-p',
  project,
  '-f',
  join(process.cwd(), 'docker-compose.yml'),
  '-f',
  join(process.cwd(), 'docker-compose.ha.yml'),
  '-f',
  join(process.cwd(), 'docker-compose.operations.yml'),
  '-f',
  join(process.cwd(), 'docker-compose.reliability.yml'),
  '--profile',
  'ha-redis',
  '--profile',
  'operations',
];
const compose = (...args) => docker([...prefix, ...args]);
const exec = (service, code) =>
  compose('exec', '-T', service, 'node', '--input-type=module', '-e', code);
const json = (output) =>
  JSON.parse(
    output
      .trim()
      .split('\n')
      .findLast((line) => line.startsWith('{')),
  );
const wait = async (probe, description, seconds = 90) => {
  const deadline = Date.now() + seconds * 1000;
  let lastError;
  while (Date.now() < deadline) {
    try {
      if (await probe()) return;
    } catch (error) {
      lastError = error;
    }
    await sleep(500);
  }
  throw new Error(`Timeout: ${description}${lastError ? ` (${lastError.message})` : ''}`);
};
const api = async (path, admin = false) => {
  const output = exec(
    'backup',
    `
    import process from 'node:process'; import {Buffer} from 'node:buffer';
    const headers = ${admin ? "{Authorization:'Basic '+Buffer.from('admin:'+process.env.DRILL_ADMIN_PASSWORD).toString('base64')}" : "{'X-API-Key':process.env.DRILL_API_KEY}"};
    const r=await fetch('http://app:3000${path}',{headers,signal:AbortSignal.timeout(5000)});
    process.stdout.write(JSON.stringify({status:r.status,body:await r.json()}));`,
  );
  return json(output);
};
const objects = () =>
  json(
    exec(
      'backup',
      "const r=await fetch('http://fixture-s3:9000/objects'); process.stdout.write(JSON.stringify({objects:await r.json()}));",
    ),
  ).objects;
const reports = [];
let cleanupError;
let failure;
try {
  compose('build', 'app', 'backup');
  compose('up', '-d', '--wait', '--wait-timeout', '180');
  const steady = json(
    compose(
      'exec',
      '-T',
      '-e',
      `LOAD_API_KEY=${env.API_KEYS}`,
      '-e',
      `LOAD_SECONDS=${process.env.RELIABILITY_LOAD_SECONDS || 30}`,
      'backup',
      'node',
      'ops/reliability/load.js',
    ),
  );
  reports.push(steady);
  const pulse = json(
    compose(
      'exec',
      '-T',
      '-e',
      `LOAD_API_KEY=${env.API_KEYS}`,
      '-e',
      'LOAD_RATE=100',
      '-e',
      'LOAD_SECONDS=5',
      'backup',
      'node',
      'ops/reliability/load.js',
    ),
  );
  reports.push(pulse);
  compose('stop', 'fixture-s3');
  const audited = await api('/admin/api/dead-letter', true);
  if (audited.status !== 200) throw new Error('Archive outage broke admin API');
  await wait(
    async () =>
      json(compose('exec', '-T', 'backup', 'node', 'ops/reliability/probe.js', 'counts')).failed >
      0,
    'persisted audit retry backlog',
  );
  compose('restart', 'app');
  compose('start', 'fixture-s3');
  await wait(
    async () => objects().some((object) => object.key.endsWith('.json')),
    'archived audit after outage/restart',
  );
  reports.push({
    kind: 'audit-recovery-report',
    apiStatusDuringOutage: audited.status,
    restartRecovered: true,
  });
  compose('stop', 'app');
  const unrouted = json(
    compose('exec', '-T', 'backup', 'node', 'ops/reliability/probe.js', 'unrouted'),
  );
  compose('start', 'app');
  await wait(async () => {
    const result = await api('/admin/api/dead-letter', true);
    return result.status === 200 && JSON.stringify(result.body).includes(unrouted.taskId);
  }, 'reconciled missing DLQ');
  reports.push({ ...unrouted, reconciled: true });
  const beforeFailover = Date.now();
  compose('stop', 'redis');
  await wait(async () => {
    const role = compose(
      'exec',
      '-T',
      'redis-replica',
      'sh',
      '-c',
      'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli role',
    );
    return role.startsWith('master');
  }, 'Sentinel replica promotion');
  await wait(
    async () => (await api('/health/ready')).status === 200,
    'API readiness after actual container-stop failover',
  );
  const after = json(
    compose(
      'exec',
      '-T',
      '-e',
      `LOAD_API_KEY=${env.API_KEYS}`,
      '-e',
      'LOAD_SECONDS=5',
      'backup',
      'node',
      'ops/reliability/load.js',
    ),
  );
  if ((await api(`/api/v1/tasks/${pulse.verifiedTaskId}`)).status !== 200)
    throw new Error('Prior task lost across failover');
  reports.push({
    kind: 'failover-report',
    seconds: (Date.now() - beforeFailover) / 1000,
    newTasks: after.completed,
    priorTaskReadable: true,
  });
  compose('exec', '-T', 'backup', 'node', 'ops/reliability/probe.js', 'pause');
  if ((await api('/admin/api/dead-letter', true)).status !== 200)
    throw new Error('Cannot seed pending audit');
  compose('stop', 'app');
  const sample = json(
    compose(
      'exec',
      '-T',
      'backup',
      'node',
      'ops/reliability/probe.js',
      'seed-sample',
      unrouted.taskId,
    ),
  );
  const snapshot = json(
    compose('exec', '-T', 'backup', 'node', 'dist/src/operations/backup-cli.js', 'once'),
  );
  const restored = json(
    compose(
      'exec',
      '-T',
      '-e',
      `RESTORE_SAMPLE_MANIFEST=${JSON.stringify(sample.manifest)}`,
      'backup',
      'node',
      'dist/src/operations/backup-cli.js',
      'restore-check',
      snapshot.key,
      snapshot.versionId,
    ),
  );
  if (Object.values(restored.auditCounts).reduce((sum, count) => sum + count, 0) < 1)
    throw new Error('Restore lost pending audit outbox');
  if (Object.values(restored.taskCounts).reduce((sum, count) => sum + count, 0) < 1)
    throw new Error('Restore lost task state');
  if (restored.verifiedSamples !== 7) throw new Error('Restore did not verify every sample');
  reports.push(
    {
      kind: 'representative-data-report',
      states: sample.manifest.jobs.map((job) => job.state),
      verifiedSamples: restored.verifiedSamples,
    },
    snapshot,
    restored,
  );
  const exportDirectory = exec(
    'backup',
    "import {mkdtemp} from 'node:fs/promises';process.stdout.write(await mkdtemp('/tmp/dtq-drill-export-'));",
  ).trim();
  try {
    const exported = json(
      compose(
        'exec',
        '-T',
        'backup',
        'node',
        'dist/src/operations/backup-cli.js',
        'restore-export',
        snapshot.key,
        exportDirectory,
        snapshot.versionId,
      ),
    );
    compose('exec', '-T', 'backup', 'redis-check-rdb', `${exportDirectory}/dump.rdb`);
    reports.push(exported);
  } finally {
    exec(
      'backup',
      `import {unlink,rmdir} from 'node:fs/promises';await unlink('${exportDirectory}/dump.rdb').catch(error=>{if(error.code!=='ENOENT')throw error;});await rmdir('${exportDirectory}');`,
    );
  }
  compose('exec', '-T', 'backup', 'node', 'ops/reliability/probe.js', 'resume');
} catch (error) {
  failure = error;
} finally {
  try {
    compose('down', '-v', '--remove-orphans', '--timeout', '30');
  } catch (error) {
    cleanupError = error;
    process.stderr.write(`Owned fixture cleanup failed: ${error.message}\n`);
  }
  for (const image of [env.APP_IMAGE, env.OPERATIONS_IMAGE]) {
    try {
      docker(['image', 'rm', image]);
    } catch (error) {
      cleanupError = error;
      process.stderr.write(`Owned image cleanup failed: ${error.message}\n`);
    }
  }
}
const report = {
  kind: 'reliability-report',
  passed: !failure && !cleanupError,
  project,
  subnet,
  completedAt: new Date().toISOString(),
  reports,
};
if (process.env.RELIABILITY_REPORT)
  await writeFile(process.env.RELIABILITY_REPORT, JSON.stringify(report, null, 2));
process.stdout.write(`${JSON.stringify(report)}\n`);
if (failure) throw failure;
if (cleanupError) throw cleanupError;
