import { randomUUID } from 'node:crypto';
import { Queue, Worker } from 'bullmq';
import type { Redis } from 'ioredis';
import type { Logger } from 'pino';
import { z } from 'zod';
import type { AppConfig } from '../config/env.js';
import { AUDIT_ACTIONS, AUDIT_OUTCOMES, type AuditRecord, type AuditSink } from './audit.js';
import type { AuditWormSink } from './audit-worm.js';
import { createMaintenanceLoop } from '../operations/maintenance.js';
import {
  auditDeliveryTotal,
  auditOutboxJobs,
  auditOutboxOldestAge,
  maintenanceLastSuccess,
} from './metrics.js';

const recordSchema = z.object({
  archiveId: z.uuid(),
  timestamp: z.iso.datetime(),
  action: z.enum(AUDIT_ACTIONS),
  outcome: z.enum(AUDIT_OUTCOMES),
  actor: z.string().nullable(),
  requestId: z.string(),
  method: z.string(),
  path: z.string(),
  subject: z.string().nullable().optional(),
  role: z.string().nullable().optional(),
  ip: z.string().optional(),
  statusCode: z.number().int().optional(),
  tenantId: z.string().optional(),
  taskId: z.string().optional(),
  newTaskId: z.string().optional(),
  operationId: z.string().optional(),
  reason: z.string().optional(),
  id: z.string().optional(),
});
type OutboxRecord = z.infer<typeof recordSchema>;

export interface AuditOutbox extends AuditSink {
  ready: () => Promise<void>;
  reconcile: () => Promise<void>;
}

export const createAuditOutbox = (
  settings: AppConfig,
  producer: Redis,
  consumer: Redis,
  archive: Pick<AuditWormSink, 'deliver' | 'close'>,
  logger: Logger,
): AuditOutbox => {
  const name = 'audit-worm-outbox';
  const queue = new Queue<OutboxRecord>(name, {
    connection: producer,
    prefix: `${settings.SERVICE_NAME}:bull`,
    defaultJobOptions: {
      attempts: settings.AUDIT_OUTBOX_ATTEMPTS,
      backoff: { type: 'exponential', delay: settings.AUDIT_OUTBOX_BACKOFF_MS },
      removeOnComplete: true,
      removeOnFail: false,
    },
  });
  const worker = new Worker<OutboxRecord>(
    name,
    async (job) => {
      try {
        const record = recordSchema.parse(job.data);
        await archive.deliver(record);
        auditDeliveryTotal.inc({ outcome: 'archived' });
      } catch (error: unknown) {
        auditDeliveryTotal.inc({ outcome: 'retry' });
        throw error;
      }
    },
    {
      connection: consumer,
      prefix: `${settings.SERVICE_NAME}:bull`,
      concurrency: settings.AUDIT_OUTBOX_CONCURRENCY,
    },
  );
  const logError = (error: Error): void => {
    logger.error({ errorType: error.name }, 'Audit outbox Redis/worker error');
  };
  queue.on('error', logError);
  worker.on('error', logError);
  let closed = false;
  const maintenance = createMaintenanceLoop(
    'audit-outbox',
    settings.MAINTENANCE_INTERVAL_MS,
    async () => {
      const failed = await queue.getJobs(['failed'], 0, settings.MAINTENANCE_BATCH_SIZE - 1, true);
      for (const job of failed) await job.retry('failed', { resetAttemptsMade: true });
      const counts = await queue.getJobCounts('waiting', 'active', 'delayed', 'failed', 'paused');
      for (const [state, value] of Object.entries(counts)) auditOutboxJobs.set({ state }, value);
      const pending = (
        await Promise.all(
          (['waiting', 'active', 'delayed', 'failed', 'paused'] as const).map((state) =>
            queue.getJobs([state], 0, 0, true),
          ),
        )
      ).flat();
      const oldest = pending.reduce((minimum, job) => Math.min(minimum, job.timestamp), Date.now());
      auditOutboxOldestAge.set(Math.max(0, (Date.now() - oldest) / 1_000));
      maintenanceLastSuccess.set({ operation: 'audit-outbox' }, Date.now() / 1_000);
    },
    logger,
  );

  return {
    durable: true,
    async ready() {
      await Promise.all([queue.waitUntilReady(), worker.waitUntilReady()]);
      await maintenance.run();
    },
    reconcile: maintenance.run,
    async write(record: AuditRecord) {
      const archiveId = record.archiveId ?? randomUUID();
      try {
        if (closed) throw new Error('OutboxClosed');
        const data = recordSchema.parse({ ...record, archiveId });
        await queue.add('archive', data, { jobId: archiveId });
        auditDeliveryTotal.inc({ outcome: 'enqueued' });
        logger.info(
          { archiveId, operationId: data.operationId },
          'Audit event queued for WORM archival',
        );
      } catch (error: unknown) {
        auditDeliveryTotal.inc({ outcome: 'enqueue_failed' });
        logger.error(
          { archiveId, errorType: error instanceof Error ? error.name : 'OutboxError' },
          'Audit outbox enqueue failed; event remains in logs/Redis audit if available',
        );
      }
    },
    async close() {
      closed = true;
      await maintenance.close();
      await worker.close();
      await queue.close();
      await archive.close();
    },
  };
};
