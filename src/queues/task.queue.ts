import { Queue } from 'bullmq';
import { config } from '../config/env.js';
import { redis } from '../config/redis.js';
import type { TaskJobData, TaskResult, TaskType } from '../types/task.types.js';

export const TASK_QUEUE_NAME = 'tasks';
export const TASK_MAX_ATTEMPTS = config.TASK_MAX_ATTEMPTS;

export const taskQueue = new Queue<TaskJobData, TaskResult, TaskType>(TASK_QUEUE_NAME, {
  connection: redis,
  defaultJobOptions: {
    attempts: TASK_MAX_ATTEMPTS,
    backoff: {
      type: 'exponential',
      delay: config.TASK_BACKOFF_DELAY_MS,
    },
    removeOnComplete: {
      age: 24 * 60 * 60,
      count: 1_000,
    },
    removeOnFail: config.DLQ_RECONCILE_ENABLED
      ? false
      : {
          age: 7 * 24 * 60 * 60,
          count: 5_000,
        },
  },
});
