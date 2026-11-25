import { UnrecoverableError, type Job } from 'bullmq';
import { SpanKind } from '@opentelemetry/api';
import type { Logger } from 'pino';
import type { BillingGateway } from '../services/billing.mock.service.js';
import { effectKey, type RedisEffectStore } from '../services/effect-idempotency.js';
import { payloadFingerprint } from '../services/task-fingerprint.js';
import { processDocument } from '../services/document.service.js';
import { withTimeout } from '../resilience/timeout.js';
import { rememberJobAttempt, withJobSpan, withSpan } from '../observability/tracing.js';
import {
  taskPayloadSchema,
  type TaskJobData,
  type TaskResult,
  type TaskType,
  PUBLIC_TENANT_ID,
} from '../types/task.types.js';

export type TaskJob = Job<TaskJobData, TaskResult, TaskType>;

export interface TaskProcessorDependencies {
  billing: BillingGateway;
  billingCircuit: { execute: <T>(operation: () => Promise<T>) => Promise<T> };
  effects: Pick<RedisEffectStore, 'begin' | 'complete'>;
  effectNamespace: string;
  timeoutMs: number;
  logger: Logger;
}

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

    const span = {
      'messaging.message.id': job.id ?? '',
      'task.id': job.id ?? '',
      'task.type': job.name,
      'task.attempt': attempt,
      ...(job.data.meta?.requestId ? { 'request.id': job.data.meta.requestId } : {}),
      ...(job.data.meta?.tenantId ? { 'tenant.id': job.data.meta.tenantId } : {}),
    };
    return withJobSpan(`process ${job.name}`, job.data.meta, span, () => {
      rememberJobAttempt(job);
      // Redis is a trust boundary: invalid jobs fail once rather than being retried.
      const parsed = taskPayloadSchema.safeParse(job.data);
      if (!parsed.success) {
        const reason = parsed.error.issues
          .map(({ path, message }) => `${path.join('.')}: ${message}`)
          .join('; ');
        log.error({ reason }, 'Rejecting job with invalid payload');
        throw new UnrecoverableError(`Invalid task payload: ${reason}`);
      }
      const payload = parsed.data;
      return withTimeout(async (signal) => {
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
            const effectId = job.data.meta?.effectId ?? job.data.meta?.replayOf ?? job.id;
            if (!effectId) throw new UnrecoverableError('Invoice task has no effect identity');
            const key = effectKey(
              dependencies.effectNamespace,
              job.data.meta?.tenantId ?? PUBLIC_TENANT_ID,
              effectId,
            );
            const fingerprint = payloadFingerprint(payload);
            log.info({ customerId: payload.customerId }, 'Invoice processing started');
            try {
              const prior = await dependencies.effects.begin(key, fingerprint);
              if (prior) return prior;
              const result = await dependencies.billingCircuit.execute(() =>
                withSpan('billing.processInvoice', SpanKind.CLIENT, {}, () =>
                  dependencies.billing.processInvoice(payload, signal, { key, fingerprint }),
                ),
              );
              const completed = await dependencies.effects.complete(key, fingerprint, result);
              log.info(
                { customerId: payload.customerId, invoiceId: completed.invoiceId },
                'Invoice processed successfully',
              );
              return completed;
            } catch (error: unknown) {
              log.warn(
                { customerId: payload.customerId, err: error },
                'Invoice processing failed; BullMQ will handle retries',
              );
              throw error;
            }
          }
        }
      }, dependencies.timeoutMs);
    });
  };
