import { Queue } from 'bullmq';
import { redis } from '../config/redis.js';
import type { TaskPayload, TaskResult, TaskType } from '../types/task.types.js';

export const TASK_QUEUE_NAME = 'tasks';

export const taskQueue = new Queue<TaskPayload, TaskResult, TaskType>(TASK_QUEUE_NAME, {
  connection: redis,
  defaultJobOptions: {
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 2_000,
    },
    removeOnComplete: {
      age: 24 * 60 * 60,
      count: 1_000,
    },
    removeOnFail: {
      age: 7 * 24 * 60 * 60,
      count: 5_000,
    },
  },
});
