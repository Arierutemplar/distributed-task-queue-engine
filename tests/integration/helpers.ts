import type { Express } from 'express';
import { createApp } from '../../src/app.js';
import {
  createReadinessChecks,
  redisRateLimitStoreFactory,
  waitForRedisReady,
} from '../../src/bootstrap.js';
import { logger } from '../../src/config/logger.js';
import { coordinationRedis, pingRedis, redis } from '../../src/config/redis.js';
import {
  AUDIT_STREAM_KEY_SUFFIX,
  createAuditLog,
  type AuditLog,
} from '../../src/observability/audit.js';
import { metricsRegistry } from '../../src/observability/metrics.js';
import { deadLetterQueue } from '../../src/queues/dead-letter.queue.js';
import { taskQueue } from '../../src/queues/task.queue.js';
import type { TaskState } from '../../src/types/task.types.js';
import { ADMIN_PASSWORD, ADMIN_USERNAME, SERVICE_NAME } from './constants.js';

export const adminAuthHeader = `Basic ${Buffer.from(`${ADMIN_USERNAME}:${ADMIN_PASSWORD}`).toString('base64')}`;

export const AUDIT_STREAM_KEY = `${SERVICE_NAME}:${AUDIT_STREAM_KEY_SUFFIX}`;

export const createIntegrationAuditLog = (): AuditLog =>
  createAuditLog({ logger, redis, streamKey: AUDIT_STREAM_KEY });

/** The production composition (Redis rate-limit store, Redis readiness) minus the listener. */
export const buildApp = (
  options: { auditLog?: AuditLog; isShuttingDown?: () => boolean } = {},
): Promise<Express> =>
  waitForRedisReady(coordinationRedis, 10_000).then(() =>
    createApp({
      readinessChecks: createReadinessChecks({
        isShuttingDown: options.isShuttingDown ?? (() => false),
        pingRedis: () => pingRedis(1_000),
      }),
      createRateLimitStore: redisRateLimitStoreFactory(coordinationRedis, SERVICE_NAME),
      auditLog: options.auditLog ?? createIntegrationAuditLog(),
    }),
  );

/** Fresh state for every test: empty Redis database and zeroed metrics. */
export const resetState = async (): Promise<void> => {
  await redis.flushdb();
  metricsRegistry.resetMetrics();
};

export const closeConnections = async (): Promise<void> => {
  await taskQueue.close();
  await deadLetterQueue.close();
  await coordinationRedis.quit();
  await redis.quit();
};

export const waitFor = async <T>(
  probe: () => Promise<T | undefined | null | false>,
  description: string,
  timeoutMs = 15_000,
): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined && value !== null && value !== false) {
      return value;
    }
    if (Date.now() > deadline) {
      throw new Error(`Timed out after ${timeoutMs}ms waiting for ${description}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
};

export const waitForJobState = (taskId: string, states: TaskState[]): Promise<TaskState> =>
  waitFor(
    async () => {
      const job = await taskQueue.getJob(taskId);
      const state = job ? ((await job.getState()) as TaskState) : undefined;
      return state && states.includes(state) ? state : undefined;
    },
    `task ${taskId} to reach ${states.join('|')}`,
  );

/** Reads one sample from the Prometheus registry; extra default labels are ignored. */
export const metricValue = async (
  name: string,
  labels: Record<string, string> = {},
): Promise<number | undefined> => {
  const metric = (await metricsRegistry.getMetricsAsJSON()).find((entry) => entry.name === name);
  const sample = metric?.values.find((value) =>
    Object.entries(labels).every(([key, expected]) => String(value.labels[key]) === expected),
  );
  return sample?.value;
};

export const documentPayload = {
  type: 'PROCESS_DOCUMENT',
  documentId: 'doc-1',
  content: 'hello integration world',
};
export const notificationPayload = {
  type: 'SEND_NOTIFICATION',
  recipient: 'ops@example.com',
  subject: 'Integration',
  message: 'Queued through real Redis',
};
export const invoicePayload = {
  type: 'PROCESS_INVOICE',
  customerId: 'customer-1',
  amount: 42,
  currency: 'EUR',
  taxId: 'TAX-1',
  items: [{ description: 'Item', quantity: 2, unitPrice: 21 }],
};
