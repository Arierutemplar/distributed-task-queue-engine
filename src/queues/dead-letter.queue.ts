import { Queue } from 'bullmq';
import { redis } from '../config/redis.js';
import type { DeadLetterJobData } from '../types/task.types.js';

export const DEAD_LETTER_QUEUE_NAME = 'tasks-dead-letter';
export const DEAD_LETTER_JOB_NAME = 'dead-letter';

export const deadLetterJobId = (originalJobId: string): string => `dlq-${originalJobId}`;

// Parking queue: no worker consumes it. Entries stay in "waiting" until an operator
// replays or deletes them through the admin API, so they outlive the failed-set retention.
export const deadLetterQueue = new Queue<DeadLetterJobData, void, typeof DEAD_LETTER_JOB_NAME>(
  DEAD_LETTER_QUEUE_NAME,
  {
    connection: redis,
    defaultJobOptions: {
      attempts: 1,
      removeOnComplete: true,
      removeOnFail: false,
    },
  },
);
