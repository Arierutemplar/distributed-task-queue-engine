import { createHash } from 'node:crypto';
import { SpanKind, trace } from '@opentelemetry/api';
import { currentTraceContext, withSpan } from '../observability/tracing.js';
import { deadLetterJobId, deadLetterQueue } from '../queues/dead-letter.queue.js';
import { taskQueue } from '../queues/task.queue.js';
import {
  PUBLIC_TENANT_ID,
  type TaskAcceptedResponse,
  type TaskJobMeta,
  type TaskPayload,
  type TaskState,
  type TaskStatusResponse,
} from '../types/task.types.js';

export class IdempotencyConflictError extends Error {
  readonly statusCode = 409;

  constructor() {
    super('Idempotency-Key was already used with a different payload');
    this.name = 'IdempotencyConflictError';
  }
}

export interface DispatchOptions {
  requestId: string;
  tenantId: string;
  idempotencyKey?: string;
  replayOf?: string;
}

export interface DispatchResult {
  response: TaskAcceptedResponse;
  replayed: boolean;
}

const sha256 = (value: string): string => createHash('sha256').update(value, 'utf8').digest('hex');

// Zod emits keys in schema order, so JSON.stringify of a parsed payload is deterministic.
export const payloadFingerprint = (payload: TaskPayload): string => sha256(JSON.stringify(payload));

// BullMQ custom ids may not contain ":" or be integers; a hex digest avoids both.
// The tenant is part of the digest so two tenants can use the same key independently.
export const idempotentJobId = (tenantId: string, idempotencyKey: string): string =>
  `idem-${sha256(`${tenantId}\n${idempotencyKey}`)}`;

const ownerOf = (meta: TaskJobMeta | undefined): string => meta?.tenantId ?? PUBLIC_TENANT_ID;

const toIso = (epochMs: number | undefined): string | null =>
  epochMs ? new Date(epochMs).toISOString() : null;

export const dispatchTask = async (
  payload: TaskPayload,
  options: DispatchOptions,
): Promise<DispatchResult> =>
  withSpan(
    'enqueue task',
    SpanKind.PRODUCER,
    { 'request.id': options.requestId, 'tenant.id': options.tenantId, 'task.type': payload.type },
    async () => {
      const meta: TaskJobMeta = {
        requestId: options.requestId,
        tenantId: options.tenantId,
        payloadHash: payloadFingerprint(payload),
        acceptedAt: new Date().toISOString(),
        ...(options.idempotencyKey ? { idempotencyKey: options.idempotencyKey } : {}),
        ...(options.replayOf ? { replayOf: options.replayOf } : {}),
        ...currentTraceContext(),
      };
      const jobId = options.idempotencyKey
        ? idempotentJobId(options.tenantId, options.idempotencyKey)
        : undefined;

      const job = await taskQueue.add(
        payload.type,
        { ...payload, meta },
        jobId ? { jobId } : undefined,
      );

      if (!job.id) {
        throw new Error('Queue accepted the task without assigning a job ID');
      }
      trace.getActiveSpan()?.setAttribute('task.id', job.id);

      let acceptedAt = meta.acceptedAt;
      let replayed = false;

      if (jobId) {
        // Queue.add with an existing jobId is a no-op in Redis but returns our local data,
        // so the stored job is the source of truth for replay and conflict detection.
        const stored = await taskQueue.getJob(job.id);
        const storedMeta = stored?.data.meta;
        if (storedMeta) {
          if (storedMeta.payloadHash !== meta.payloadHash) {
            throw new IdempotencyConflictError();
          }
          replayed = storedMeta.requestId !== meta.requestId;
          acceptedAt = storedMeta.acceptedAt;
        }
      }

      return {
        response: {
          taskId: job.id,
          taskType: payload.type,
          status: 'accepted',
          acceptedAt,
        },
        replayed,
      };
    },
  );

/** Returns null for unknown tasks and for tasks owned by another tenant, so ids cannot be probed. */
export const getTaskStatus = async (
  taskId: string,
  tenantId: string,
): Promise<TaskStatusResponse | null> => {
  const job = await taskQueue.getJob(taskId);

  if (job) {
    if (ownerOf(job.data.meta) !== tenantId) {
      return null;
    }
    const state = (await job.getState()) as TaskState;
    // returnvalue is typed as the result but is null until the job completes.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    const result = job.returnvalue ?? null;
    return {
      taskId,
      taskType: job.name,
      state,
      attemptsMade: job.attemptsMade,
      maxAttempts: job.opts.attempts ?? 1,
      acceptedAt: job.data.meta?.acceptedAt ?? toIso(job.timestamp),
      processedAt: toIso(job.processedOn),
      finishedAt: toIso(job.finishedOn),
      result,
      failedReason: job.failedReason || null,
    };
  }

  // The failed set is trimmed by retention; the dead-letter entry outlives it.
  const deadLetter = await deadLetterQueue.getJob(deadLetterJobId(taskId));
  if (!deadLetter || ownerOf(deadLetter.data.data.meta) !== tenantId) {
    return null;
  }
  return {
    taskId,
    taskType: deadLetter.data.taskType,
    state: 'dead-lettered',
    attemptsMade: deadLetter.data.attemptsMade,
    maxAttempts: deadLetter.data.attemptsMade,
    acceptedAt: deadLetter.data.data.meta?.acceptedAt ?? null,
    processedAt: null,
    finishedAt: deadLetter.data.failedAt,
    result: null,
    failedReason: deadLetter.data.failedReason,
  };
};
