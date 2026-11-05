import { taskQueue } from '../queues/task.queue.js';
import type { TaskAcceptedResponse, TaskPayload } from '../types/task.types.js';

export const dispatchTask = async (
  payload: TaskPayload,
): Promise<TaskAcceptedResponse> => {
  const job = await taskQueue.add(payload.type, payload);

  if (!job.id) {
    throw new Error('Queue accepted the task without assigning a job ID');
  }

  return {
    taskId: job.id,
    taskType: payload.type,
    status: 'accepted',
    acceptedAt: new Date().toISOString(),
  };
};
