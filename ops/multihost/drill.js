import { spawnSync } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { join } from 'node:path';
import process from 'node:process';
import { setTimeout as sleep } from 'node:timers/promises';

const requested = process.argv.slice(2);
if (
  requested.length > 1 ||
  (requested[0] && !['host-a', 'host-b', 'both'].includes(requested[0]))
) {
  throw new Error('Usage: node ops/multihost/drill.js [host-a|host-b|both]');
}
const scenarios = requested[0] && requested[0] !== 'both' ? [requested[0]] : ['host-a', 'host-b'];
const project = `dtq-mh-${randomUUID().replaceAll('-', '').slice(0, 12)}`;
const image = `${project}:fixture`;
const env = {
  ...process.env,
  DTQ_MH_PROJECT: project,
  DTQ_MH_IMAGE: image,
  DTQ_MH_REDIS_PASSWORD: randomBytes(24).toString('hex'),
  DTQ_MH_API_KEY: `fixture-${randomBytes(24).toString('hex')}`,
};

const docker = (args, allowFailure = false) => {
  const result = spawnSync('docker', args, {
    env,
    encoding: 'utf8',
    maxBuffer: 8 * 1024 * 1024,
    timeout: 600_000,
  });
  if (result.error || result.status !== 0) {
    if (allowFailure) return undefined;
    throw new Error(
      `Docker command failed: ${result.error?.message ?? `${result.stderr.slice(-4000)}\n${result.stdout.slice(-4000)}`}`,
    );
  }
  return result.stdout.trim();
};

const projects = (docker(['compose', 'ls', '--all', '--quiet']) ?? '').split(/\s+/u);
if (projects.includes(project) || docker(['image', 'inspect', image], true) !== undefined) {
  throw new Error('Generated fixture project or image already exists; refusing to reuse it');
}

const networkIds = (docker(['network', 'ls', '-q']) ?? '').split(/\s+/u).filter(Boolean);
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
const subnet = Array.from({ length: 7 }, (_, index) => `172.${25 + index}.253.0/24`).find(
  (candidate) => {
    const [start, end] = range(candidate);
    return occupied.every(([first, last]) => end < first || start > last);
  },
);
if (!subnet) throw new Error('No free fixture subnet; no resources created');
const ip = (last) => subnet.replace('0/24', String(last));
Object.assign(env, {
  DTQ_MH_SUBNET: subnet,
  DTQ_MH_REDIS_A_IP: ip(10),
  DTQ_MH_REDIS_B_IP: ip(11),
  DTQ_MH_SENTINEL_A_IP: ip(21),
  DTQ_MH_SENTINEL_B_IP: ip(22),
  DTQ_MH_SENTINEL_W_IP: ip(23),
});

const prefix = [
  'compose',
  '-p',
  project,
  '-f',
  join(process.cwd(), 'docker-compose.multihost.yml'),
];
const compose = (...args) => docker([...prefix, ...args]);
const execNode = (service, code) =>
  compose('exec', '-T', service, 'node', '--input-type=module', '-e', code);
const wait = async (probe, description, seconds = 120) => {
  const deadline = Date.now() + seconds * 1_000;
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

const domains = {
  'host-a': ['redis-a', 'sentinel-a', 'api-a', 'worker-a'],
  'host-b': ['redis-b', 'sentinel-b', 'api-b', 'worker-b'],
  witness: ['sentinel-witness'],
};
const configured = JSON.parse(compose('config', '--format', 'json')).services;
for (const [service, settings] of Object.entries(configured)) {
  const domain = settings.labels?.['dtq.failure-domain'];
  if (!domains[domain]?.includes(service))
    throw new Error(`Unexpected failure domain for ${service}`);
}
for (const [domain, services] of Object.entries(domains)) {
  if (services.some((service) => configured[service]?.labels?.['dtq.failure-domain'] !== domain)) {
    throw new Error(`Incomplete ${domain} failure domain`);
  }
}

const sentinelMaster = (sentinel) =>
  compose(
    'exec',
    '-T',
    sentinel,
    'redis-cli',
    '--raw',
    '-p',
    '26379',
    'SENTINEL',
    'get-master-addr-by-name',
    'task-queue',
  ).split(/\r?\n/u)[0];
const quorum = (sentinel) =>
  compose(
    'exec',
    '-T',
    sentinel,
    'redis-cli',
    '--raw',
    '-p',
    '26379',
    'SENTINEL',
    'CKQUORUM',
    'task-queue',
  ).startsWith('OK');
const discoveredTopology = (sentinel) => {
  const fields = compose(
    'exec',
    '-T',
    sentinel,
    'redis-cli',
    '--raw',
    '-p',
    '26379',
    'SENTINEL',
    'master',
    'task-queue',
  ).split(/\r?\n/u);
  const info = Object.fromEntries(
    fields.filter((_, index) => index % 2 === 0).map((key, index) => [key, fields[index * 2 + 1]]),
  );
  return Number(info['num-other-sentinels']) >= 2 && Number(info['num-slaves']) >= 1;
};
const role = (service) =>
  compose(
    'exec',
    '-T',
    service,
    'sh',
    '-c',
    'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli --raw INFO replication',
  );
const healthy = (service) => {
  const output = execNode(
    service,
    "const r=await fetch('http://127.0.0.1:3000/health/ready',{signal:AbortSignal.timeout(2000)}); process.stdout.write(String(r.status));",
  );
  return output === '200';
};
const api = (service, path, payload) =>
  JSON.parse(
    execNode(
      service,
      `const r=await fetch('http://127.0.0.1:3000${path}',{
        method:${JSON.stringify(payload ? 'POST' : 'GET')},
        headers:{'X-API-Key':process.env.API_KEYS,'Content-Type':'application/json'},
        ${payload ? `body:JSON.stringify(${JSON.stringify(payload)}),` : ''}
        signal:AbortSignal.timeout(4000)});
      process.stdout.write(JSON.stringify({status:r.status,body:await r.json()}));`,
    ),
  );
const submit = (service, content) => {
  const response = api(service, '/api/v1/tasks/process', {
    type: 'PROCESS_DOCUMENT',
    documentId: randomUUID(),
    content,
  });
  if (response.status !== 202 || typeof response.body.taskId !== 'string')
    throw new Error(`Submission failed on ${service}: ${response.status}`);
  return { id: response.body.taskId, hash: createHash('sha256').update(content).digest('hex') };
};
const verifyTask = async (service, task) => {
  await wait(() => {
    const result = api(service, `/api/v1/tasks/${task.id}`);
    return (
      result.status === 200 &&
      result.body.state === 'completed' &&
      result.body.result?.sha256 === task.hash
    );
  }, `verified completion of ${task.id} on ${service}`);
};
const syncReplica = (service) => {
  const result = execNode(
    service,
    `import {Redis} from 'ioredis';
    const client=new Redis({
      sentinels:process.env.REDIS_SENTINEL_HOSTS.split(',').map(s=>{const [host,port]=s.split(':');return {host,port:Number(port)};}),
      name:process.env.REDIS_SENTINEL_MASTER_NAME,password:process.env.REDIS_PASSWORD,maxRetriesPerRequest:1
    });
    try {
      await client.set('dtq-multihost:replication-barrier',${JSON.stringify(randomUUID())});
      const count=await client.wait(1,10000);
      if(count<1) throw new Error('Replica did not acknowledge the replication barrier');
      process.stdout.write(String(count));
    } finally { client.disconnect(); }`,
  );
  if (Number(result) < 1) throw new Error('No replica acknowledged the barrier');
};

let started = false;
let built = false;
let failure;
let cleanupFailure;
const reports = [];
try {
  compose('build', 'api-a');
  built = true;
  started = true;
  compose('up', '-d', '--no-build', '--wait', '--wait-timeout', '180');
  await wait(
    () =>
      ['sentinel-a', 'sentinel-b', 'sentinel-witness'].every(
        (sentinel) =>
          sentinelMaster(sentinel) === ip(10) && quorum(sentinel) && discoveredTopology(sentinel),
      ) && role('redis-b').includes('master_link_status:up'),
    'three Sentinel voters and initial synchronized replica',
  );

  const tasks = [];
  for (const failedHost of scenarios) {
    const failed = failedHost === 'host-a' ? 'a' : 'b';
    const survivor = failed === 'a' ? 'b' : 'a';
    const survivingApi = `api-${survivor}`;
    const survivingWorker = `worker-${survivor}`;
    const survivingRedis = `redis-${survivor}`;
    const survivingSentinel = `sentinel-${survivor}`;
    const expectedMaster = survivor === 'a' ? ip(10) : ip(11);
    const before = submit(`api-${failed}`, `before-${failedHost}-${randomUUID()}`);
    tasks.push(before);
    await verifyTask(`api-${failed}`, before);
    syncReplica(`api-${failed}`);
    await wait(
      () =>
        ['sentinel-a', 'sentinel-b', 'sentinel-witness'].every(
          (sentinel) => quorum(sentinel) && discoveredTopology(sentinel),
        ),
      'all Sentinels discovering the other voters and a replica',
    );

    const start = Date.now();
    compose('kill', '--signal', 'SIGKILL', ...domains[failedHost]);
    const output = compose('ps', '--all', '--format', 'json');
    const entries = output.startsWith('[')
      ? JSON.parse(output)
      : output.split(/\r?\n/u).filter(Boolean).map(JSON.parse);
    const state = Object.fromEntries(entries.map((item) => [item.Service, item.State]));
    if (domains[failedHost].some((service) => state[service] !== 'exited')) {
      throw new Error(`A ${failedHost} service is missing or did not exit after host loss`);
    }
    await wait(
      () =>
        role(survivingRedis).includes('role:master') &&
        sentinelMaster(survivingSentinel) === expectedMaster &&
        sentinelMaster('sentinel-witness') === expectedMaster &&
        quorum(survivingSentinel) &&
        quorum('sentinel-witness') &&
        healthy(survivingApi) &&
        healthy(survivingWorker),
      `${failedHost} loss: survivor quorum, master and readiness`,
    );
    const during = submit(survivingApi, `during-${failedHost}-${randomUUID()}`);
    tasks.push(during);
    for (const task of tasks) await verifyTask(survivingApi, task);

    compose('start', `redis-${failed}`, `sentinel-${failed}`);
    await wait(() => {
      const info = role(`redis-${failed}`);
      return (
        info.includes('role:slave') &&
        info.includes(`master_host:${expectedMaster}`) &&
        info.includes('master_link_status:up')
      );
    }, `${failedHost} Redis rejoining as a synchronized replica`);
    await wait(
      () =>
        ['sentinel-a', 'sentinel-b', 'sentinel-witness'].every(
          (sentinel) => sentinelMaster(sentinel) === expectedMaster && quorum(sentinel),
        ),
      'all Sentinels agreeing on the promoted master',
    );
    syncReplica(survivingApi);
    compose('start', `api-${failed}`, `worker-${failed}`);
    await wait(
      () => healthy(`api-${failed}`) && healthy(`worker-${failed}`),
      'rejoined API and worker readiness',
    );
    const after = submit(`api-${failed}`, `after-${failedHost}-${randomUUID()}`);
    tasks.push(after);
    for (const task of tasks) {
      await verifyTask(survivingApi, task);
      await verifyTask(`api-${failed}`, task);
    }
    syncReplica(survivingApi);
    reports.push({
      failedHost,
      promotedMaster: survivingRedis,
      recovered: true,
      checkedTasks: tasks.length,
      elapsedSeconds: (Date.now() - start) / 1000,
    });
  }
} catch (error) {
  failure = error;
  if (started) {
    try {
      process.stderr.write(
        `${compose('ps', '--all')}\n${compose('logs', '--tail', '60', 'redis-a', 'redis-b', 'sentinel-a', 'sentinel-b', 'sentinel-witness')}\n`,
      );
    } catch (diagnosticError) {
      process.stderr.write(`Fixture diagnostics failed: ${diagnosticError.message}\n`);
    }
  }
} finally {
  if (started) {
    try {
      compose('down', '-v', '--remove-orphans', '--timeout', '30');
    } catch (error) {
      cleanupFailure = error;
    }
  }
  if (built) {
    try {
      docker(['image', 'rm', image]);
    } catch (error) {
      cleanupFailure = error;
    }
  }
}
process.stdout.write(
  `${JSON.stringify({ kind: 'multihost-report', project, subnet, passed: !failure && !cleanupFailure, reports })}\n`,
);
if (failure) throw failure;
if (cleanupFailure) throw cleanupFailure;
