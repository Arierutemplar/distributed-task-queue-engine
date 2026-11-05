import type { RequestHandler } from 'express';
import { taskPayloadSchema } from '../types/task.types.js';
import { dispatchTask } from '../services/task.service.js';

export const processTask: RequestHandler = async (request, response, next) => {
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
    const result = await dispatchTask(parsedPayload.data);
    response.status(202).json(result);
  } catch (error: unknown) {
    next(error);
  }
};
