import { DelayedError, Worker } from 'bullmq';
import { SpanKind, trace } from '@opentelemetry/api';
import { config } from '../config/env.js';
import { logger } from '../config/logger.js';
import { coordinationRedis, redis } from '../config/redis.js';
import {
  taskDuration,
  taskEndToEndDuration,
  tasksProcessedTotal,
  setCircuitState,
  fairShareDeferrals,
} from '../observability/metrics.js';
import { jobAttemptContext, withJobSpan, withTraceContext } from '../observability/tracing.js';
import { addDeadLetter } from '../queues/dead-letter.queue.js';
import { TASK_QUEUE_NAME } from '../queues/task.queue.js';
import { acquireTenantSlot, releaseTenantSlot } from '../queues/tenant-slots.js';
import { RedisCircuitBreaker } from '../resilience/redis-circuit-breaker.js';
import { BillingMockService, type BillingGateway } from '../services/billing.mock.service.js';
import { RedisEffectStore } from '../services/effect-idempotency.js';
import {
  PUBLIC_TENANT_ID,
  type TaskJobData,
  type TaskResult,
  type TaskType,
} from '../types/task.types.js';
import { createTaskProcessor, type TaskJob } from './task.processor.js';

const BILLING_CIRCUIT = 'billing';

// finishedOn is only set on terminal outcomes; intermediate failed attempts use "now".
const attemptSeconds = (job: TaskJob): number | undefined =>
  job.processedOn ? ((job.finishedOn ?? Date.now()) - job.processedOn) / 1_000 : undefined;

/**
 * Copies a job that reached the terminal failed state into the dead-letter queue.
 * The deterministic DLQ job id makes this idempotent if two workers observe the same failure.
 */
export const routeToDeadLetter = async (job: TaskJob, error: Error): Promise<boolean> => {
  if (!job.id) {
    return false;
  }
  const taskId = job.id;

  return withJobSpan(
    'dead-letter.route',
    jobAttemptContext(job) ?? job.data.meta,
    {
      'task.id': taskId,
      'task.type': job.name,
      ...(job.data.meta?.requestId ? { 'request.id': job.data.meta.requestId } : {}),
      ...(job.data.meta?.tenantId ? { 'tenant.id': job.data.meta.tenantId } : {}),
    },
    async () => {
      const routed = await addDeadLetter({
        originalJobId: taskId,
        taskType: job.name,
        data: job.data,
        failedReason: job.failedReason || error.message,
        attemptsMade: job.attemptsMade,
        failedAt: new Date(job.finishedOn ?? Date.now()).toISOString(),
      });
      trace.getActiveSpan()?.setAttribute('dead_letter.routed', routed);
      if (!routed) return false;
      logger.error(
        {
          jobId: job.id,
          taskType: job.name,
          attemptsMade: job.attemptsMade,
          tenantId: job.data.meta?.tenantId,
          requestId: job.data.meta?.requestId,
          err: error,
        },
        'Task exhausted its attempts and was moved to the dead-letter queue',
      );
      return true;
    },
    SpanKind.PRODUCER,
  );
};

export interface TaskWorkerOptions {
  /** Billing gateway; defaults to the mock configured by BILLING_FAILURE_RATE. */
  billing?: BillingGateway;
  concurrency?: number;
  activeSlots?: number;
  timeoutMs?: number;
  circuitBreaker?: { failureThreshold?: number; resetTimeoutMs?: number };
}

export const createTaskWorker = (
  options: TaskWorkerOptions = {},
): Worker<TaskJobData, TaskResult, TaskType> => {
  const timeoutMs = options.timeoutMs ?? config.JOB_TIMEOUT_MS;
  const billingCircuit = new RedisCircuitBreaker({
    redis: coordinationRedis,
    key: `${config.SERVICE_NAME}:circuit:billing`,
    name: BILLING_CIRCUIT,
    failureThreshold:
      options.circuitBreaker?.failureThreshold ?? config.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    resetTimeoutMs:
      options.circuitBreaker?.resetTimeoutMs ?? config.CIRCUIT_BREAKER_RESET_TIMEOUT_MS,
    probeLeaseMs: timeoutMs + 1_000,
    onStateChange: (state, previous) => {
      setCircuitState(BILLING_CIRCUIT, state);
      if (state !== previous) {
        logger.warn({ circuit: BILLING_CIRCUIT, state, previous }, 'Circuit breaker state changed');
      }
    },
  });

  const effects = new RedisEffectStore(
    coordinationRedis,
    config.SERVICE_NAME,
    config.EFFECT_RETENTION_SECONDS,
  );
  const processor = createTaskProcessor({
    billing:
      options.billing ??
      new BillingMockService({ failureRate: config.BILLING_FAILURE_RATE, store: effects }),
    billingCircuit,
    effects,
    effectNamespace: config.SERVICE_NAME,
    timeoutMs,
    logger,
  });

  const activeSlots = options.activeSlots ?? config.TENANT_MAX_ACTIVE_SLOTS;
  const workerProcessor =
    activeSlots === 0
      ? processor
      : async (job: TaskJob, token?: string) => {
          if (!job.id || !token) throw new Error('Missing job identity or lock token');
          const tenantId = job.data.meta?.tenantId ?? PUBLIC_TENANT_ID;
          const acquired = await acquireTenantSlot(
            tenantId,
            job.id,
            token,
            activeSlots,
            timeoutMs + 30_000,
          );
          if (!acquired) {
            await job.moveToDelayed(Date.now() + 100 + Math.floor(Math.random() * 100), token);
            fairShareDeferrals.inc({ tenant: tenantId });
            throw new DelayedError();
          }
          try {
            return await processor(job);
          } finally {
            await releaseTenantSlot(tenantId, job.id, token);
          }
        };
  const worker = new Worker<TaskJobData, TaskResult, TaskType>(TASK_QUEUE_NAME, workerProcessor, {
    connection: redis,
    concurrency: options.concurrency ?? config.WORKER_CONCURRENCY,
  });

  worker.on('completed', (job, result) => {
    taskEndToEndDuration.observe(
      { task_type: job.name, outcome: 'completed' },
      Math.max(0, ((job.finishedOn ?? Date.now()) - job.timestamp) / 1_000),
    );
    const seconds = attemptSeconds(job);
    tasksProcessedTotal.inc({ task_type: job.name, outcome: 'completed' });
    if (seconds !== undefined) {
      taskDuration.observe({ task_type: job.name, outcome: 'completed' }, seconds);
    }
    withTraceContext(jobAttemptContext(job) ?? job.data.meta, () => {
      logger.info(
        {
          jobId: job.id,
          taskType: job.name,
          attempt: job.attemptsMade,
          maxAttempts: job.opts.attempts ?? 1,
          durationSeconds: seconds,
          requestId: job.data.meta?.requestId,
          result,
        },
        'Task completed',
      );
    });
  });

  worker.on('failed', (job, error) => {
    if (!job) {
      logger.error({ err: error }, 'Task failed without job context');
      return;
    }
    const seconds = attemptSeconds(job);
    if (job.finishedOn !== undefined) {
      taskEndToEndDuration.observe(
        { task_type: job.name, outcome: 'failed' },
        Math.max(0, (job.finishedOn - job.timestamp) / 1_000),
      );
    }
    tasksProcessedTotal.inc({ task_type: job.name, outcome: 'failed' });
    if (seconds !== undefined) {
      taskDuration.observe({ task_type: job.name, outcome: 'failed' }, seconds);
    }
    withTraceContext(jobAttemptContext(job) ?? job.data.meta, () => {
      logger.error(
        {
          jobId: job.id,
          taskType: job.name,
          attempt: job.attemptsMade,
          maxAttempts: job.opts.attempts ?? 1,
          requestId: job.data.meta?.requestId,
          err: error,
        },
        'Task attempt failed',
      );
    });
    void routeToDeadLetter(job, error).catch((dlqError: unknown) => {
      withTraceContext(jobAttemptContext(job) ?? job.data.meta, () => {
        logger.error(
          { jobId: job.id, requestId: job.data.meta?.requestId, err: dlqError },
          'Failed to move task to the dead-letter queue',
        );
      });
    });
  });

  worker.on('stalled', (jobId) => {
    logger.warn({ jobId }, 'Task stalled and will be retried');
  });

  worker.on('error', (error) => {
    logger.error({ err: error }, 'Task worker error');
  });

  return worker;
};
