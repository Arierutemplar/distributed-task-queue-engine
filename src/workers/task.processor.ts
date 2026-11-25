import { createHash } from 'node:crypto';
import { UnrecoverableError, type Job } from 'bullmq';
import type { Logger } from 'pino';
import type { BillingGateway } from '../services/billing.mock.service.js';
import { withTimeout } from '../resilience/timeout.js';
import { withJobSpan } from '../observability/tracing.js';
import {
  taskPayloadSchema,
  type TaskJobData,
  type TaskPayload,
  type TaskResult,
  type TaskType,
} from '../types/task.types.js';

export type TaskJob = Job<TaskJobData, TaskResult, TaskType>;

export interface TaskProcessorDependencies {
  billing: BillingGateway;
  billingCircuit: { execute: <T>(operation: () => Promise<T>) => Promise<T> };
  timeoutMs: number;
  logger: Logger;
}

const processDocument = (
  payload: Extract<TaskPayload, { type: 'PROCESS_DOCUMENT' }>,
): TaskResult => {
  const trimmedContent = payload.content.trim();
  return {
    taskType: 'PROCESS_DOCUMENT',
    documentId: payload.documentId,
    characterCount: Array.from(payload.content).length,
    wordCount: trimmedContent.length === 0 ? 0 : trimmedContent.split(/\s+/u).length,
    sha256: createHash('sha256').update(payload.content, 'utf8').digest('hex'),
    completedAt: new Date().toISOString(),
  };
};

export const createTaskProcessor =
  (dependencies: TaskProcessorDependencies) =>
  async (job: TaskJob): Promise<TaskResult> => {
    const attempt = job.attemptsMade + 1;
    const log = dependencies.logger.child({
      jobId: job.id,
      taskType: job.name,
      attempt,
      requestId: job.data.meta?.requestId,
    });

    // The API validated this payload, but Redis is a trust boundary: a malformed job
    // (manual insert, schema drift between deploys) must fail once instead of retrying.
    const parsed = taskPayloadSchema.safeParse(job.data);
    if (!parsed.success) {
      const reason = parsed.error.issues
        .map(({ path, message }) => `${path.join('.')}: ${message}`)
        .join('; ');
      log.error({ reason }, 'Rejecting job with invalid payload');
      throw new UnrecoverableError(`Invalid task payload: ${reason}`);
    }
    const payload = parsed.data;

    const span = {
      'messaging.message.id': job.id ?? '',
      'task.type': job.name,
      'task.attempt': attempt,
    };
    return withJobSpan(`process ${job.name}`, job.data.meta, span, () =>
      withTimeout(async (signal) => {
        switch (payload.type) {
          case 'PROCESS_DOCUMENT': {
            const result = processDocument(payload);
            log.info({ documentId: payload.documentId }, 'Document processed');
            return result;
          }
          case 'SEND_NOTIFICATION': {
            log.info('Notification task processed');
            return {
              taskType: 'SEND_NOTIFICATION',
              notificationId: job.id ?? `notification-${job.timestamp}`,
              status: 'processed',
              processedAt: new Date().toISOString(),
            };
          }
          case 'PROCESS_INVOICE': {
            log.info({ customerId: payload.customerId }, 'Invoice processing started');
            try {
              const result = await dependencies.billingCircuit.execute(() =>
                dependencies.billing.processInvoice(payload, signal),
              );
              log.info(
                { customerId: payload.customerId, invoiceId: result.invoiceId },
                'Invoice processed successfully',
              );
              return result;
            } catch (error: unknown) {
              log.warn(
                { customerId: payload.customerId, err: error },
                'Invoice processing failed; BullMQ will handle retries',
              );
              throw error;
            }
          }
        }
      }, dependencies.timeoutMs),
    );
  };
