import '../config/dotenv.js';
import { spawn, type ChildProcess } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { createServer as tcpServer, type AddressInfo } from 'node:net';
import { constants } from 'node:fs';
import { copyFile, mkdtemp, readdir, realpath, rmdir, stat, unlink } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { tmpdir } from 'node:os';
import { Redis } from 'ioredis';
import { Queue, QueueEvents, Worker } from 'bullmq';
import { Gauge, Counter, Registry } from 'prom-client';
import { loadSecrets, SecretsProviderError } from '../config/secrets.js';
import { loadRedisSettings } from '../config/redis-settings.js';
import { resolveRedisConnection } from '../config/redis-options.js';
import { withTimeout } from '../resilience/timeout.js';
import { processDocument } from '../services/document.service.js';
import {
  taskPayloadSchema,
  type TaskJobData,
  type TaskResult,
  type TaskType,
} from '../types/task.types.js';
import {
  archiveSnapshot,
  downloadSnapshot,
  BackupError,
  createBackupStorage,
  loadBackupSettings,
} from './backup.js';
import { restoreManifestSchema, verifyRestoreSamples } from './restore-samples.js';

const execute = (
  program: string,
  args: string[],
  signal: AbortSignal,
  env = process.env,
): Promise<void> =>
  new Promise((resolve, reject) => {
    const child = spawn(program, args, { env, signal, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout.resume();
    child.stderr.resume();
    let failure: Error | undefined;
    child.once('error', (error) => {
      failure = error;
    });
    child.once('close', (code) => {
      if (failure) {
        reject(failure);
        return;
      }
      if (code === 0) resolve();
      else reject(new BackupError(`${program} failed with exit ${code}`));
    });
  });
const cleanup = async (directory: string): Promise<void> => {
  for (const entry of await readdir(directory)) await unlink(join(directory, entry));
  await rmdir(directory);
};
const freePort = async (): Promise<number> => {
  const server = tcpServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as AddressInfo).port;
  await new Promise<void>((resolve, reject) =>
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    }),
  );
  return port;
};
const waitFor = async (operation: () => Promise<boolean>, signal: AbortSignal): Promise<void> => {
  while (!signal.aborted) {
    if (await operation()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new BackupError('Restore verification aborted');
};
const stopChild = async (child: ChildProcess): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => child.kill('SIGKILL'), 5_000);
    child.once('exit', () => {
      clearTimeout(timer);
      resolve();
    });
    child.kill('SIGTERM');
  });
};

const runWithDeadline = async <T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number,
): Promise<T> => {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort(new BackupError('Operation deadline exceeded'));
  }, timeoutMs);
  try {
    const result = await operation(controller.signal);
    controller.signal.throwIfAborted();
    return result;
  } finally {
    clearTimeout(timer);
  }
};

const main = async (): Promise<void> => {
  await loadSecrets();
  const settings = loadBackupSettings(process.env);
  const storage = createBackupStorage(settings);
  const verifyRdb = (file: string, signal: AbortSignal): Promise<void> =>
    execute('redis-check-rdb', [file], signal);

  const capture = async (signal: AbortSignal) => {
    const redisSettings = loadRedisSettings(process.env);
    const connection = resolveRedisConnection(redisSettings, {
      lazyConnect: true,
      maxRetriesPerRequest: 1,
      connectTimeout: 5_000,
      retryStrategy: () => 250,
    });
    const client = connection.url
      ? new Redis(connection.url, connection.options)
      : new Redis(connection.options);
    const directory = await mkdtemp(join(tmpdir(), 'dtq-backup-'));
    const abort = (): void => {
      client.disconnect();
    };
    signal.addEventListener('abort', abort, { once: true });
    try {
      signal.throwIfAborted();
      await withTimeout(() => client.connect(), 10_000);
      const role = await client.role();
      if (role[0] !== 'master')
        throw new BackupError('Snapshot source must be the discovered master');
      const info = await client.call('CLIENT', 'INFO');
      if (typeof info !== 'string')
        throw new BackupError('Redis did not return its listening address');
      const address = /(?:^|\s)laddr=(\S+)/u.exec(info)?.[1];
      if (!address) throw new BackupError('Cannot discover snapshot source address');
      const colon = address.lastIndexOf(':');
      const host = address.slice(0, colon).replace(/^\[|\]$/gu, '');
      const port = address.slice(colon + 1);
      const file = join(directory, 'snapshot.rdb');
      const args = ['-h', host, '-p', port, '--rdb', file];
      if (client.options.username) args.push('--user', client.options.username);
      if (client.options.tls) {
        args.push('--tls', '--sni', client.options.host ?? redisSettings.REDIS_HOST);
        if (process.env.BACKUP_REDIS_CA_FILE)
          args.push('--cacert', process.env.BACKUP_REDIS_CA_FILE);
      }
      const capturedAt = new Date();
      await execute('redis-cli', args, signal, {
        ...process.env,
        REDISCLI_AUTH: client.options.password ?? '',
      });
      return await archiveSnapshot(
        settings,
        storage,
        file,
        directory,
        capturedAt,
        (path) => verifyRdb(path, signal),
        signal,
      );
    } finally {
      signal.removeEventListener('abort', abort);
      client.disconnect();
      await cleanup(directory);
    }
  };

  const restore = async (key: string, signal: AbortSignal, versionId?: string) => {
    const started = Date.now();
    const directory = await mkdtemp(join(tmpdir(), 'dtq-restore-'));
    let server: ChildProcess | undefined;
    let client: Redis | undefined;
    let queue: Queue<TaskJobData, TaskResult, TaskType> | undefined;
    let events: QueueEvents | undefined;
    let worker: Worker<TaskJobData, TaskResult, TaskType> | undefined;
    try {
      const snapshot = await downloadSnapshot(
        settings,
        storage,
        key,
        directory,
        (path) => verifyRdb(path, signal),
        signal,
        versionId,
      );
      const port = await freePort();
      const password = randomUUID();
      server = spawn('redis-server', ['-'], { stdio: ['pipe', 'ignore', 'ignore'], signal });
      const serverFailure = new Promise<never>((_resolve, reject) => {
        server?.once('error', reject);
        server?.once('exit', () => {
          reject(new BackupError('Isolated restore Redis exited early'));
        });
      });
      // This process can only bind loopback and loads into its own new directory.
      server.stdin?.end(
        `bind 127.0.0.1\nport ${port}\ndir ${JSON.stringify(directory)}\n` +
          `dbfilename restore.rdb\nappendonly no\nsave ""\nrequirepass ${password}\n`,
      );
      client = new Redis({
        host: '127.0.0.1',
        port,
        password,
        maxRetriesPerRequest: null,
        retryStrategy: () => 100,
      });
      client.on('error', () => {
        /* Startup readiness reports connection errors through the bounded probe. */
      });
      const restored = client;
      await Promise.race([
        waitFor(async () => {
          if (restored.status !== 'ready') return false;
          await restored.ping();
          return true;
        }, signal),
        serverFailure,
      ]);
      const original = new Queue('tasks', { connection: client });
      const outbox = new Queue('audit-worm-outbox', {
        connection: client,
        prefix: `${settings.SERVICE_NAME}:bull`,
      });
      const taskCounts = await original.getJobCounts(
        'waiting',
        'active',
        'delayed',
        'failed',
        'completed',
      );
      const auditCounts = await outbox.getJobCounts(
        'waiting',
        'active',
        'delayed',
        'failed',
        'paused',
      );
      await Promise.all([original.close(), outbox.close()]);
      const sampleInput = process.env.RESTORE_SAMPLE_MANIFEST;
      let verifiedSamples: number | undefined;
      if (sampleInput) {
        if (sampleInput.length > 32_768) throw new BackupError('Restore sample manifest too large');
        const manifest = restoreManifestSchema.parse(JSON.parse(sampleInput));
        await verifyRestoreSamples(client, settings.SERVICE_NAME, manifest);
        verifiedSamples = manifest.jobs.length + 1;
      }
      const name = `restore-probe-${randomUUID()}`;
      queue = new Queue<TaskJobData, TaskResult, TaskType>(name, { connection: client });
      events = new QueueEvents(name, { connection: client });
      worker = new Worker<TaskJobData, TaskResult, TaskType>(
        name,
        (job) => {
          const data = taskPayloadSchema.parse(job.data);
          if (data.type !== 'PROCESS_DOCUMENT')
            throw new BackupError('Unexpected restore probe task');
          return Promise.resolve(processDocument(data));
        },
        { connection: client },
      );
      await Promise.race([
        Promise.all([events.waitUntilReady(), worker.waitUntilReady()]),
        serverFailure,
      ]);
      const content = 'verified restore document';
      const job = await queue.add('PROCESS_DOCUMENT', {
        type: 'PROCESS_DOCUMENT',
        documentId: randomUUID(),
        content,
      });
      const result = await Promise.race([
        job.waitUntilFinished(events, settings.BACKUP_RTO_SECONDS * 1_000),
        serverFailure,
      ]);
      if (
        result.taskType !== 'PROCESS_DOCUMENT' ||
        result.sha256 !== createHash('sha256').update(content).digest('hex')
      )
        throw new BackupError('Restored worker did not produce the expected document result');
      const report = {
        kind: 'restore-report',
        key,
        versionId: versionId ?? null,
        capturedAt: snapshot.capturedAt.toISOString(),
        rpoSeconds: Math.max(0, (started - snapshot.capturedAt.getTime()) / 1_000),
        technicalRtoSeconds: (Date.now() - started) / 1_000,
        taskCounts,
        auditCounts,
        documentProbe: 'completed',
        ...(verifiedSamples === undefined ? {} : { verifiedSamples }),
      };
      process.stdout.write(`${JSON.stringify(report)}\n`);
      if (
        report.rpoSeconds > settings.BACKUP_RPO_SECONDS ||
        report.technicalRtoSeconds > settings.BACKUP_RTO_SECONDS
      )
        throw new BackupError('Restore drill exceeded its configured RPO/RTO objective');
    } finally {
      await worker?.close(true);
      await events?.close();
      await queue?.close();
      client?.disconnect();
      if (server) await stopChild(server);
      await cleanup(directory);
    }
  };

  const mode = process.argv[2] ?? 'once';
  if (mode === 'restore-export') {
    try {
      const key = process.argv[3];
      const destination = process.argv[4];
      const versionId = process.argv.at(5);
      if (!key || !destination || !isAbsolute(destination))
        throw new BackupError(
          'restore-export requires an object key and an absolute empty directory',
        );
      const resolved = await realpath(destination);
      if (!(await stat(resolved)).isDirectory() || (await readdir(resolved)).length !== 0)
        throw new BackupError(
          'Restore destination must be an existing empty directory on a new volume',
        );
      await runWithDeadline(async (signal) => {
        const directory = await mkdtemp(join(tmpdir(), 'dtq-export-'));
        try {
          const snapshot = await downloadSnapshot(
            settings,
            storage,
            key,
            directory,
            (path) => verifyRdb(path, signal),
            signal,
            versionId,
          );
          const rpoSeconds = Math.max(0, (Date.now() - snapshot.capturedAt.getTime()) / 1000);
          if (rpoSeconds > settings.BACKUP_RPO_SECONDS)
            throw new BackupError(
              'Snapshot exceeds the RPO objective; explicit incident review required',
            );
          signal.throwIfAborted();
          await copyFile(snapshot.file, join(resolved, 'dump.rdb'), constants.COPYFILE_EXCL);
          signal.throwIfAborted();
          process.stdout.write(
            `${JSON.stringify({ kind: 'restore-export-report', key, versionId: versionId ?? null, capturedAt: snapshot.capturedAt.toISOString(), rpoSeconds, exported: true })}\n`,
          );
        } finally {
          await cleanup(directory);
        }
      }, settings.BACKUP_RTO_SECONDS * 1000);
    } finally {
      storage.close();
    }
    return;
  }
  if (mode === 'once' || mode === 'restore-check') {
    try {
      if (mode === 'restore-check') {
        const key = process.argv[3];
        if (!key) throw new BackupError('restore-check requires an object key');
        await runWithDeadline(
          (signal) => restore(key, signal, process.argv[4]),
          settings.BACKUP_RTO_SECONDS * 1_000,
        );
      } else {
        const report = await runWithDeadline(capture, settings.BACKUP_TIMEOUT_MS);
        process.stdout.write(`${JSON.stringify({ kind: 'backup-report', ...report })}\n`);
      }
    } finally {
      storage.close();
    }
    return;
  }
  if (mode !== 'agent') {
    storage.close();
    throw new BackupError('Mode must be once, agent, restore-check or restore-export');
  }
  const registry = new Registry();
  registry.setDefaultLabels({ service: settings.SERVICE_NAME, role: 'backup' });
  const last = new Gauge({
    name: 'backup_last_verified_timestamp_seconds',
    help: 'Capture time of the last remotely verified snapshot',
    registers: [registry],
  });
  const failures = new Counter({
    name: 'backup_failures_total',
    help: 'Failed snapshot upload/verification runs',
    registers: [registry],
  });
  let lastVerified = 0;
  let running: Promise<void> | undefined;
  const run = (): Promise<void> => {
    if (running) return running;
    running = runWithDeadline(capture, settings.BACKUP_TIMEOUT_MS)
      .then((report) => {
        lastVerified = Date.parse(report.capturedAt) / 1_000;
        last.set(lastVerified);
        process.stdout.write(`${JSON.stringify({ kind: 'backup-report', ...report })}\n`);
      })
      .catch((error: unknown) => {
        failures.inc();
        process.stderr.write(
          `${JSON.stringify({
            level: 50,
            msg: 'Backup capture/verification failed',
            errorType: error instanceof BackupError ? error.message : 'BackupOperationError',
          })}\n`,
        );
      })
      .finally(() => {
        running = undefined;
      });
    return running;
  };
  const http = createServer((request, response) => {
    void (async () => {
      if (request.url === '/metrics') {
        response.setHeader('Content-Type', registry.contentType);
        response.end(await registry.metrics());
      } else if (request.url === '/health/ready') {
        const ready =
          lastVerified > 0 && Date.now() / 1_000 - lastVerified <= settings.BACKUP_RPO_SECONDS;
        response.statusCode = ready ? 200 : 503;
        response.end(JSON.stringify({ ready, lastVerified }));
      } else {
        response.statusCode = 404;
        response.end();
      }
    })().catch(() => {
      process.stderr.write('{"level":50,"msg":"Backup HTTP handler failed"}\n');
      response.statusCode = 500;
      response.end('Backup metrics unavailable');
    });
  });
  await new Promise<void>((resolve, reject) => {
    http.once('error', reject);
    http.listen(settings.BACKUP_PORT, '0.0.0.0', resolve);
  });
  const timer = setInterval(() => void run(), settings.BACKUP_INTERVAL_MS);
  let stopping: Promise<void> | undefined;
  const stop = (): Promise<void> =>
    (stopping ??= (async () => {
      clearInterval(timer);
      await running;
      storage.close();
      await new Promise<void>((resolve, reject) =>
        http.close((error) => {
          if (error) reject(error);
          else resolve();
        }),
      );
    })());
  for (const signal of ['SIGINT', 'SIGTERM'] as const)
    process.once(signal, () => {
      void stop().catch(() => {
        process.stderr.write('{"level":50,"msg":"Backup shutdown failed"}\n');
        process.exitCode = 1;
      });
    });
  void run();
};

void main().catch((error: unknown) => {
  process.stderr.write(
    `${JSON.stringify({
      level: 60,
      msg: 'Backup operation failed',
      detail:
        error instanceof BackupError || error instanceof SecretsProviderError
          ? error.message
          : 'Invalid configuration, storage or tool failure',
    })}\n`,
  );
  process.exitCode = 1;
});
