import type { Server } from 'node:http';
import type { Worker } from 'bullmq';
import { createApp } from './app.js';
import {
  closeServer,
  createGracefulShutdown,
  createReadinessChecks,
  redisRateLimitStoreFactory,
  type ShutdownStep,
} from './bootstrap.js';
import { config, runsApi, runsWorker } from './config/env.js';
import { logger } from './config/logger.js';
import { coordinationRedis, pingRedis, redis } from './config/redis.js';
import { AUDIT_STREAM_KEY_SUFFIX, createAuditLog } from './observability/audit.js';
import { initTracing, type TracingHandle } from './observability/tracing.js';
import { deadLetterQueue } from './queues/dead-letter.queue.js';
import { taskQueue } from './queues/task.queue.js';
import type { TaskJobData, TaskResult, TaskType } from './types/task.types.js';
import { createTaskWorker } from './workers/task.worker.js';

let worker: Worker<TaskJobData, TaskResult, TaskType> | undefined;
let server: Server | undefined;
let tracing: TracingHandle | undefined;

const lifecycle = createGracefulShutdown({
  logger,
  timeoutMs: config.SHUTDOWN_TIMEOUT_MS,
  onTimeout: () => process.exit(1),
  steps: () => {
    const steps: ShutdownStep[] = [];
    const activeServer = server;
    const activeWorker = worker;
    const activeTracing = tracing;
    if (activeServer) {
      steps.push({ name: 'HTTP server', close: () => closeServer(activeServer) });
    }
    if (activeWorker) {
      steps.push({ name: 'task worker', close: () => activeWorker.close() });
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

const startServer = async (): Promise<void> => {
  logger.info(
    { role: config.PROCESS_ROLE, nodeEnv: config.NODE_ENV, concurrency: config.WORKER_CONCURRENCY },
    'Starting task service',
  );
  if (runsApi(config.PROCESS_ROLE) && !config.ADMIN_PASSWORD) {
    logger.warn('ADMIN_PASSWORD is not set; /admin/queues and /admin/api are disabled');
  }
  if (runsApi(config.PROCESS_ROLE) && config.API_KEYS.length === 0) {
    logger.warn(
      'API_KEYS is not set; the task API is unauthenticated (allowed outside production only)',
    );
  }

  tracing = await initTracing({
    enabled: config.OTEL_ENABLED,
    serviceName: config.SERVICE_NAME,
    role: config.PROCESS_ROLE,
  });
  if (tracing) {
    logger.info('OpenTelemetry tracing enabled (OTLP/HTTP exporter)');
  }

  await pingRedis(10_000);

  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.on(signal, () => {
      void shutdown(signal);
    });
  }

  if (runsWorker(config.PROCESS_ROLE)) {
    worker = createTaskWorker();
  }

  const app = await createApp({
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
      redis,
      streamKey: `${config.SERVICE_NAME}:${AUDIT_STREAM_KEY_SUFFIX}`,
      maxEntries: config.AUDIT_LOG_MAX_ENTRIES,
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

void startServer().catch(async (error: unknown) => {
  logger.fatal({ err: error }, 'Failed to start task service');
  process.exitCode = 1;
  await shutdown('startup failure');
});
