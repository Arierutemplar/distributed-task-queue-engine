import type { RequestHandler } from 'express';
import { getRequestId, getTenantId } from '../middleware/http.middleware.js';
import { tasksEnqueuedTotal } from '../observability/metrics.js';
import { dispatchTask, getTaskStatus } from '../services/task.service.js';
import { idempotencyKeySchema, taskIdSchema, taskPayloadSchema } from '../types/task.types.js';

export const processTask: RequestHandler = async (request, response, next) => {
  const rawKey = request.header('idempotency-key');
  const parsedKey = rawKey === undefined ? undefined : idempotencyKeySchema.safeParse(rawKey);
  if (parsedKey && !parsedKey.success) {
    response.status(400).json({
      error: 'Invalid Idempotency-Key header',
      issues: parsedKey.error.issues.map(({ message }) => ({ path: 'Idempotency-Key', message })),
    });
    return;
  }

  const parsedPayload = taskPayloadSchema.safeParse(request.body);

  if (!parsedPayload.success) {
    response.status(400).json({
      error: 'Invalid task payload',
      issues: parsedPayload.error.issues.map(({ path, message }) => ({
        path: path.join('.'),
        message,
      })),
    });
    return;
  }

  try {
    const { response: accepted, replayed } = await dispatchTask(parsedPayload.data, {
      requestId: getRequestId(response.locals),
      tenantId: getTenantId(response.locals),
      idempotencyKey: parsedKey?.data,
    });
    tasksEnqueuedTotal.inc({ task_type: accepted.taskType, replayed: String(replayed) });
    response.setHeader('Location', `${request.baseUrl}/${accepted.taskId}`);
    if (replayed) {
      response.setHeader('Idempotent-Replayed', 'true');
    }
    response.status(202).json(accepted);
  } catch (error: unknown) {
    next(error);
  }
};

export const getTask: RequestHandler = async (request, response, next) => {
  const parsedId = taskIdSchema.safeParse(request.params.taskId);
  if (!parsedId.success) {
    response.status(400).json({ error: 'Invalid task id' });
    return;
  }

  try {
    const status = await getTaskStatus(parsedId.data, getTenantId(response.locals));
    if (!status) {
      response.status(404).json({ error: 'Task not found' });
      return;
    }
    response.status(200).json(status);
  } catch (error: unknown) {
    next(error);
  }
};
