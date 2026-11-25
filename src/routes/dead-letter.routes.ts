import { Router, type Request, type Response } from 'express';
import { z } from 'zod';
import { getAdminActor, getAdminIdentity, getRequestId } from '../middleware/http.middleware.js';
import type { AuditEvent, AuditLog } from '../observability/audit.js';
import { deadLetterJobId, deadLetterQueue } from '../queues/dead-letter.queue.js';
import { dispatchTask } from '../services/task.service.js';
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
  ): Promise<void> =>
    audit.record({
      actor: getAdminActor(response.locals),
      ...getAdminIdentity(response.locals),
      requestId: getRequestId(response.locals),
      method: request.method,
      path: request.originalUrl.split('?')[0] ?? request.originalUrl,
      ip: request.ip,
      ...details,
    });

  router.get('/', async (request, response, next) => {
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
      next(error);
    }
  });

  router.post('/:taskId/replay', async (request, response, next) => {
    const action = 'dead_letter.replay';
    const parsedId = taskIdSchema.safeParse(request.params.taskId);
    if (!parsedId.success) {
      await recordAudit(request, response, { action, outcome: 'invalid', statusCode: 400 });
      response.status(400).json({ error: 'Invalid task id' });
      return;
    }
    const taskId = parsedId.data;

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
      const tenantId = entry.data.data.meta?.tenantId ?? PUBLIC_TENANT_ID;

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

      // A fresh job id: the original id may still sit in the failed set within retention.
      // The replay stays with the original tenant so its owner can still read the status.
      const { response: accepted } = await dispatchTask(payload.data, {
        requestId: getRequestId(response.locals),
        tenantId,
        replayOf: taskId,
        effectId: entry.data.data.meta?.effectId ?? entry.data.data.meta?.replayOf ?? taskId,
        limits: response.locals.admissionLimits as AdmissionLimits | undefined,
      });
      await entry.remove();
      await recordAudit(request, response, {
        action,
        outcome: 'success',
        statusCode: 202,
        taskId,
        tenantId,
        newTaskId: accepted.taskId,
      });
      response.status(202).json({ ...accepted, replayOf: taskId });
    } catch (error: unknown) {
      await recordAudit(request, response, {
        action,
        outcome: 'error',
        statusCode: error instanceof BackpressureError ? 429 : 500,
        taskId,
        reason: error instanceof Error ? error.message : 'unknown error',
      });
      next(error);
    }
  });

  router.delete('/:taskId', async (request, response, next) => {
    const action = 'dead_letter.delete';
    const parsedId = taskIdSchema.safeParse(request.params.taskId);
    if (!parsedId.success) {
      await recordAudit(request, response, { action, outcome: 'invalid', statusCode: 400 });
      response.status(400).json({ error: 'Invalid task id' });
      return;
    }
    const taskId = parsedId.data;

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
      await entry.remove();
      await recordAudit(request, response, {
        action,
        outcome: 'success',
        statusCode: 204,
        taskId,
        tenantId: entry.data.data.meta?.tenantId ?? PUBLIC_TENANT_ID,
      });
      response.status(204).end();
    } catch (error: unknown) {
      await recordAudit(request, response, {
        action,
        outcome: 'error',
        statusCode: 500,
        taskId,
        reason: error instanceof Error ? error.message : 'unknown error',
      });
      next(error);
    }
  });

  return router;
};
