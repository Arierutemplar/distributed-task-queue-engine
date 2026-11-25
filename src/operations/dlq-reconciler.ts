import type { Redis } from 'ioredis';
import type { Logger } from 'pino';
import type { AppConfig } from '../config/env.js';
import { taskQueue } from '../queues/task.queue.js';
import { addDeadLetter } from '../queues/dead-letter.queue.js';
import { dlqReconciledTotal, maintenanceLastSuccess } from '../observability/metrics.js';
import { createMaintenanceLoop, type MaintenanceLoop } from './maintenance.js';

export const createDlqReconciler = (
  settings: AppConfig,
  client: Redis,
  logger: Logger,
): MaintenanceLoop | undefined => {
  if (!settings.DLQ_RECONCILE_ENABLED) return undefined;
  let cursor = '0';
  const pending: string[] = [];
  return createMaintenanceLoop(
    'dlq-reconcile',
    settings.MAINTENANCE_INTERVAL_MS,
    async () => {
      if (pending.length === 0) {
        const [next, entries] = await client.zscan(
          taskQueue.toKey('failed'),
          cursor,
          'COUNT',
          settings.MAINTENANCE_BATCH_SIZE,
        );
        for (let index = 0; index < entries.length; index += 2) pending.push(entries[index]);
        cursor = next;
      }
      for (
        let index = 0;
        index < settings.MAINTENANCE_BATCH_SIZE && pending.length > 0;
        index += 1
      ) {
        const id = pending[0];
        const job = await taskQueue.getJob(id);
        if (!job?.id || job.finishedOn === undefined) {
          pending.shift();
          continue;
        }
        const routed = await addDeadLetter(
          {
            originalJobId: job.id,
            taskType: job.name,
            data: job.data,
            failedReason: job.failedReason,
            attemptsMade: job.attemptsMade,
            failedAt: new Date(job.finishedOn).toISOString(),
          },
          client,
        );
        pending.shift();
        if (routed) {
          dlqReconciledTotal.inc();
          logger.warn({ jobId: job.id }, 'Recovered missing dead-letter entry');
        }
      }
      maintenanceLastSuccess.set({ operation: 'dlq-reconcile' }, Date.now() / 1_000);
    },
    logger,
  );
};
