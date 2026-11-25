import { Router, type Request, type Response } from 'express';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import {
  asyncHandler,
  getAdminActor,
  getAdminIdentity,
  getRequestId,
} from '../middleware/http.middleware.js';
import type { AuditEvent, AuditLog } from '../observability/audit.js';
import { deadLetterJobId, deadLetterQueue } from '../queues/dead-letter.queue.js';
import { dispatchTask, replayJobId } from '../services/task.service.js';
import { PUBLIC_TENANT_ID, taskIdSchema, taskPayloadSchema } from '../types/task.types.js';
import type { AdmissionLimits } from '../queues/admission.js';
import { BackpressureError } from '../queues/admission.js';

const paginationSchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).default(50),
  offset: z.coerce.number().int().min(0).default(0),
});

type AuditDetails = Omit<AuditEvent, 'actor' | 'requestId' | 'method' | 'path' | 'ip'>;

export const createDeadLetterRouter = (audit: AuditLog): Router => {
  const router = Router();

  const recordAudit = (
    request: Request,
    response: Response,
    details: AuditDetails,
    options?: { requirePersistence?: boolean },
  ): Promise<void> => {
    const event: AuditEvent = {
      actor: getAdminActor(response.locals),
      ...getAdminIdentity(response.locals),
      requestId: getRequestId(response.locals),
      method: request.method,
      path: request.originalUrl.split('?')[0] ?? request.originalUrl,
      ip: request.ip,
      ...details,
    };
    return options ? audit.record(event, options) : audit.record(event);
  };

  router.get(
    '/',
    asyncHandler(async (request, response, next) => {
      const parsed = paginationSchema.safeParse(request.query);
      if (!parsed.success) {
        response.status(400).json({ error: 'Invalid pagination parameters' });
        return;
      }
      const { limit, offset } = parsed.data;

      try {
        const [total, jobs] = await Promise.all([
          deadLetterQueue.getWaitingCount(),
          deadLetterQueue.getWaiting(offset, offset + limit - 1),
        ]);
        await recordAudit(request, response, {
          action: 'dead_letter.list',
          outcome: 'success',
          statusCode: 200,
        });
        response.json({
          total,
          limit,
          offset,
          items: jobs.map((job) => ({
            taskId: job.data.originalJobId,
            taskType: job.data.taskType,
            failedReason: job.data.failedReason,
            attemptsMade: job.data.attemptsMade,
            failedAt: job.data.failedAt,
            requestId: job.data.data.meta?.requestId ?? null,
            tenantId: job.data.data.meta?.tenantId ?? PUBLIC_TENANT_ID,
          })),
        });
      } catch (error: unknown) {
        await recordAudit(request, response, {
          action: 'dead_letter.list',
          outcome: 'error',
          statusCode: 500,
          reason: 'list_failed',
        });
        next(error);
      }
    }),
  );

  router.post(
    '/:taskId/replay',
    asyncHandler(async (request, response, next) => {
      const action = 'dead_letter.replay';
      const parsedId = taskIdSchema.safeParse(request.params.taskId);
      if (!parsedId.success) {
        await recordAudit(request, response, { action, outcome: 'invalid', statusCode: 400 });
        response.status(400).json({ error: 'Invalid task id' });
        return;
      }
      const taskId = parsedId.data;
      const operationId = randomUUID();
      let newTaskId: string | undefined;
      let removed = false;
      let tenantId: string | undefined;

      try {
        const entry = await deadLetterQueue.getJob(deadLetterJobId(taskId));
        if (!entry) {
          await recordAudit(request, response, {
            action,
            outcome: 'not_found',
            statusCode: 404,
            taskId,
          });
          response.status(404).json({ error: 'Dead-letter entry not found' });
          return;
        }
        tenantId = entry.data.data.meta?.tenantId ?? PUBLIC_TENANT_ID;

        const payload = taskPayloadSchema.safeParse(entry.data.data);
        if (!payload.success) {
          await recordAudit(request, response, {
            action,
            outcome: 'invalid',
            statusCode: 422,
            taskId,
            tenantId,
            reason: 'payload no longer matches the task schema',
          });
          response
            .status(422)
            .json({ error: 'Dead-letter payload no longer matches the task schema' });
          return;
        }

        await recordAudit(
          request,
          response,
          { action, outcome: 'pending', taskId, tenantId, operationId },
          { requirePersistence: true },
        );
        const { response: accepted } = await dispatchTask(payload.data, {
          requestId: getRequestId(response.locals),
          tenantId,
          replayOf: taskId,
          jobId: replayJobId(tenantId, taskId),
          effectId: entry.data.data.meta?.effectId ?? entry.data.data.meta?.replayOf ?? taskId,
          limits: response.locals.admissionLimits as AdmissionLimits | undefined,
        });
        newTaskId = accepted.taskId;
        await entry.remove();
        removed = true;
        await recordAudit(request, response, {
          action,
          outcome: 'success',
          statusCode: 202,
          taskId,
          tenantId,
          operationId,
          newTaskId: accepted.taskId,
        });
        response.status(202).json({ ...accepted, replayOf: taskId });
      } catch (error: unknown) {
        await recordAudit(request, response, {
          action,
          outcome: 'error',
          statusCode: error instanceof BackpressureError ? 429 : 500,
          taskId,
          operationId,
          ...(tenantId ? { tenantId } : {}),
          ...(newTaskId ? { newTaskId } : {}),
          reason: removed
            ? 'audit_failed_after_replay'
            : newTaskId
              ? 'replay_enqueued_before_removal_failed'
              : 'replay_failed',
        });
        next(error);
      }
    }),
  );

  router.delete(
    '/:taskId',
    asyncHandler(async (request, response, next) => {
      const action = 'dead_letter.delete';
      const parsedId = taskIdSchema.safeParse(request.params.taskId);
      if (!parsedId.success) {
        await recordAudit(request, response, { action, outcome: 'invalid', statusCode: 400 });
        response.status(400).json({ error: 'Invalid task id' });
        return;
      }
      const taskId = parsedId.data;
      const operationId = randomUUID();
      let removed = false;
      let tenantId: string | undefined;

      try {
        const entry = await deadLetterQueue.getJob(deadLetterJobId(taskId));
        if (!entry) {
          await recordAudit(request, response, {
            action,
            outcome: 'not_found',
            statusCode: 404,
            taskId,
          });
          response.status(404).json({ error: 'Dead-letter entry not found' });
          return;
        }
        tenantId = entry.data.data.meta?.tenantId ?? PUBLIC_TENANT_ID;
        await recordAudit(
          request,
          response,
          { action, outcome: 'pending', taskId, tenantId, operationId },
          { requirePersistence: true },
        );
        await entry.remove();
        removed = true;
        await recordAudit(request, response, {
          action,
          outcome: 'success',
          statusCode: 204,
          taskId,
          tenantId,
          operationId,
        });
        response.status(204).end();
      } catch (error: unknown) {
        await recordAudit(request, response, {
          action,
          outcome: 'error',
          statusCode: 500,
          taskId,
          operationId,
          ...(tenantId ? { tenantId } : {}),
          reason: removed ? 'audit_failed_after_delete' : 'delete_failed',
        });
        next(error);
      }
    }),
  );

  return router;
};
