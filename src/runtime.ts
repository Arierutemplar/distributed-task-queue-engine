import type { Server } from 'node:http';
import type { Worker } from 'bullmq';
import { createApp } from './app.js';
import {
  closeServer,
  createGracefulShutdown,
  createReadinessChecks,
  redisRateLimitStoreFactory,
  waitForRedisReady,
  type ShutdownStep,
} from './bootstrap.js';
import { config, runsApi, runsWorker } from './config/env.js';
import { logger } from './config/logger.js';
import { coordinationRedis, pingRedis, redis } from './config/redis.js';
import { AUDIT_STREAM_KEY_SUFFIX, createAuditLog, type AuditSink } from './observability/audit.js';
import { createAuditWormSink } from './observability/audit-worm.js';
import { createAuditOutbox } from './observability/audit-outbox.js';
import { createDlqReconciler } from './operations/dlq-reconciler.js';
import type { MaintenanceLoop } from './operations/maintenance.js';
import { withTimeout } from './resilience/timeout.js';
import { initTracing, type TracingHandle } from './observability/tracing.js';
import { deadLetterQueue } from './queues/dead-letter.queue.js';
import { taskQueue } from './queues/task.queue.js';
import type { TaskJobData, TaskResult, TaskType } from './types/task.types.js';
import { createTaskWorker } from './workers/task.worker.js';
import { createSecretsReloader, type SecretsReloader } from './config/secrets-reload.js';
import type { SecretsProvider } from './config/secrets.js';

let worker: Worker<TaskJobData, TaskResult, TaskType> | undefined;
let server: Server | undefined;
let tracing: TracingHandle | undefined;
let auditSink: AuditSink | undefined;
let reconciler: MaintenanceLoop | undefined;
let refreshTimer: NodeJS.Timeout | undefined;
let secretsReloader: SecretsReloader | undefined;
let secretsProvider: SecretsProvider | undefined;

const lifecycle = createGracefulShutdown({
  logger,
  timeoutMs: config.SHUTDOWN_TIMEOUT_MS,
  onTimeout: () => process.exit(1),
  steps: () => {
    const steps: ShutdownStep[] = [];
    const activeServer = server;
    const activeWorker = worker;
    const activeTracing = tracing;
    const activeAuditSink = auditSink;
    const activeReconciler = reconciler;
    if (refreshTimer) clearInterval(refreshTimer);
    if (secretsProvider) {
      const activeProvider = secretsProvider;
      steps.push({ name: 'secrets provider', close: () => activeProvider.close() });
    }
    if (activeServer) {
      steps.push({ name: 'HTTP server', close: () => closeServer(activeServer) });
    }
    if (activeWorker) {
      steps.push({ name: 'task worker', close: () => activeWorker.close() });
    }
    if (activeReconciler)
      steps.push({ name: 'DLQ reconciliation', close: () => activeReconciler.close() });
    if (activeAuditSink) {
      steps.push({ name: 'WORM audit archive', close: () => activeAuditSink.close() });
    }
    steps.push(
      { name: 'task queue', close: () => taskQueue.close() },
      { name: 'dead-letter queue', close: () => deadLetterQueue.close() },
      { name: 'Redis coordination connection', close: () => coordinationRedis.quit() },
      { name: 'Redis connection', close: () => redis.quit() },
    );
    if (activeTracing) {
      steps.push({ name: 'tracing exporter', close: () => activeTracing.shutdown() });
    }
    return steps;
  },
});

const shutdown = async (reason: string): Promise<void> => {
  if (!(await lifecycle.shutdown(reason))) {
    process.exitCode = 1;
  }
};

const startServer = async (initialEnvironment: NodeJS.ProcessEnv): Promise<void> => {
  logger.info(
    { role: config.PROCESS_ROLE, nodeEnv: config.NODE_ENV, concurrency: config.WORKER_CONCURRENCY },
    'Starting task service',
  );
  if (
    runsApi(config.PROCESS_ROLE) &&
    config.ADMIN_AUTH_MODE === 'basic' &&
    !config.ADMIN_PASSWORD
  ) {
    logger.warn('ADMIN_PASSWORD is not set; /admin/queues and /admin/api are disabled');
  }
  if (runsApi(config.PROCESS_ROLE) && config.API_KEYS.length === 0) {
    logger.warn(
      'API_KEYS is not set; the task API is unauthenticated (allowed outside production only)',
    );
  }

  tracing = await initTracing({
    enabled: config.OTEL_ENABLED,
    endpoint: config.OTEL_EXPORTER_OTLP_ENDPOINT,
    serviceName: config.OTEL_SERVICE_NAME ?? config.SERVICE_NAME,
    role: config.PROCESS_ROLE,
  });
  if (tracing) {
    logger.info('OpenTelemetry tracing enabled (OTLP/HTTP exporter)');
  }

  await pingRedis(10_000);
  if (config.REDIS_SENTINEL_HOSTS || config.AUDIT_WORM_DELIVERY === 'outbox') {
    // Sentinel resolves duplicated clients independently; stores must not initialize too early.
    await waitForRedisReady(coordinationRedis, 10_000);
    await coordinationRedis.ping();
  }
  const archive = createAuditWormSink(config, logger);
  auditSink = archive;
  if (archive && config.AUDIT_WORM_DELIVERY === 'outbox') {
    const outbox = createAuditOutbox(config, coordinationRedis, redis, archive, logger);
    auditSink = outbox;
    await withTimeout(() => outbox.ready(), 10_000);
  }
  reconciler = createDlqReconciler(config, coordinationRedis, logger);
  await reconciler?.run();

  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.on(signal, () => {
      void shutdown(signal);
    });
  }

  if (runsWorker(config.PROCESS_ROLE)) {
    worker = createTaskWorker();
  }

  secretsReloader = createSecretsReloader(
    config.API_KEYS,
    initialEnvironment,
    secretsProvider,
    process.env,
  );
  if (archive) {
    secretsReloader.register((candidate, changed) => {
      const wormKeys = [
        'AUDIT_WORM_ACCESS_KEY_ID',
        'AUDIT_WORM_SECRET_ACCESS_KEY',
        'AUDIT_WORM_ENDPOINT',
        'AUDIT_WORM_REGION',
        'AUDIT_WORM_BUCKET',
        'AUDIT_WORM_FORCE_PATH_STYLE',
        'AUDIT_WORM_LOCK_MODE',
        'AUDIT_WORM_RETENTION_DAYS',
        'AUDIT_WORM_TIMEOUT_MS',
      ] as const;
      const applied = changed.filter((key) => (wormKeys as readonly string[]).includes(key));
      if (applied.length === 0) return { applied, commit: () => undefined };
      if (!candidate.AUDIT_WORM_ACCESS_KEY_ID || !candidate.AUDIT_WORM_SECRET_ACCESS_KEY)
        throw new Error('Reloaded WORM credentials must be complete');
      const updated = {
        ...config,
        AUDIT_WORM_ACCESS_KEY_ID: candidate.AUDIT_WORM_ACCESS_KEY_ID,
        AUDIT_WORM_SECRET_ACCESS_KEY: candidate.AUDIT_WORM_SECRET_ACCESS_KEY,
        ...(candidate.AUDIT_WORM_ENDPOINT !== undefined
          ? { AUDIT_WORM_ENDPOINT: candidate.AUDIT_WORM_ENDPOINT }
          : {}),
        AUDIT_WORM_REGION: candidate.AUDIT_WORM_REGION,
        AUDIT_WORM_BUCKET: candidate.AUDIT_WORM_BUCKET,
        AUDIT_WORM_FORCE_PATH_STYLE: candidate.AUDIT_WORM_FORCE_PATH_STYLE,
        AUDIT_WORM_LOCK_MODE: candidate.AUDIT_WORM_LOCK_MODE,
        AUDIT_WORM_RETENTION_DAYS: candidate.AUDIT_WORM_RETENTION_DAYS,
        AUDIT_WORM_TIMEOUT_MS: candidate.AUDIT_WORM_TIMEOUT_MS,
      };
      return {
        applied,
        commit: () => {
          archive.updateSettings(updated);
        },
      };
    });
  }
  if (config.SECRETS_REFRESH_INTERVAL_MS > 0) {
    const reloader = secretsReloader;
    let refreshing = false;
    refreshTimer = setInterval(() => {
      if (refreshing) return;
      refreshing = true;
      void reloader
        .reload()
        .then((result) => {
          if (result.restartRequired) {
            logger.warn({ keys: result.restartRequiredKeys }, 'Secrets require controlled restart');
          }
        })
        .catch((error: unknown) => {
          logger.error({ err: error }, 'Secrets refresh failed; retaining previous credentials');
        })
        .finally(() => {
          refreshing = false;
        });
    }, config.SECRETS_REFRESH_INTERVAL_MS);
    refreshTimer.unref();
  }

  const app = await createApp({
    secretsReloader,
    readinessChecks: createReadinessChecks({
      isShuttingDown: lifecycle.isShuttingDown,
      pingRedis: () => pingRedis(1_000),
      worker,
    }),
    createRateLimitStore: runsApi(config.PROCESS_ROLE)
      ? redisRateLimitStoreFactory(coordinationRedis, config.SERVICE_NAME)
      : undefined,
    auditLog: createAuditLog({
      logger,
      redis: config.AUDIT_WORM_DELIVERY === 'outbox' ? coordinationRedis : redis,
      streamKey: `${config.SERVICE_NAME}:${AUDIT_STREAM_KEY_SUFFIX}`,
      maxEntries: config.AUDIT_LOG_MAX_ENTRIES,
      sink: auditSink,
    }),
  });

  server = await new Promise<Server>((resolve, reject) => {
    const listeningServer = app.listen(config.PORT);
    listeningServer.once('error', reject);
    listeningServer.once('listening', () => {
      resolve(listeningServer);
    });
  });
  // Slightly above typical load balancer idle timeouts to avoid 502s on reused sockets.
  server.keepAliveTimeout = 65_000;
  server.headersTimeout = 66_000;

  logger.info({ port: config.PORT }, 'HTTP server listening');
};

process.on('unhandledRejection', (reason: unknown) => {
  logger.error({ err: reason }, 'Unhandled promise rejection');
});

process.on('uncaughtException', (error: Error) => {
  logger.fatal({ err: error }, 'Uncaught exception; shutting down');
  process.exitCode = 1;
  void shutdown('uncaughtException');
});

export const startRuntime = (
  initialEnvironment: NodeJS.ProcessEnv,
  provider?: SecretsProvider,
): void => {
  secretsProvider = provider;
  void startServer(initialEnvironment).catch(async (error: unknown) => {
    logger.fatal({ err: error }, 'Failed to start task service');
    process.exitCode = 1;
    await shutdown('startup failure');
  });
};
