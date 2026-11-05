import { createHash } from 'node:crypto';
import { Worker, type Job } from 'bullmq';
import { logger, redis } from '../config/redis.js';
import { TASK_QUEUE_NAME } from '../queues/task.queue.js';
import { BillingMockService } from '../services/billing.mock.service.js';
import type { TaskPayload, TaskResult, TaskType } from '../types/task.types.js';

const billingMockService = new BillingMockService();

const handleTask = async (
  job: Job<TaskPayload, TaskResult, TaskType>,
): Promise<TaskResult> => {
  switch (job.data.type) {
    case 'PROCESS_DOCUMENT': {
      const content = job.data.content;
      const trimmedContent = content.trim();
      const wordCount = trimmedContent.length === 0 ? 0 : trimmedContent.split(/\s+/u).length;

      const result: TaskResult = {
        taskType: 'PROCESS_DOCUMENT',
        documentId: job.data.documentId,
        characterCount: Array.from(content).length,
        wordCount,
        sha256: createHash('sha256').update(content, 'utf8').digest('hex'),
        completedAt: new Date().toISOString(),
      };

      logger.info({ jobId: job.id, documentId: job.data.documentId }, 'Document processed');
      return result;
    }
    case 'SEND_NOTIFICATION': {
      const result: TaskResult = {
        taskType: 'SEND_NOTIFICATION',
        notificationId: job.id ?? `notification-${job.timestamp}`,
        status: 'processed',
        processedAt: new Date().toISOString(),
      };

      logger.info({ jobId: job.id }, 'Notification task processed');
      return result;
    }
    case 'PROCESS_INVOICE': {
      const attempt = job.attemptsMade + 1;
      logger.info(
        { jobId: job.id, customerId: job.data.customerId, attempt },
        'Invoice processing started',
      );

      try {
        const result = await billingMockService.processInvoice(job.data);
        logger.info(
          { jobId: job.id, customerId: job.data.customerId, invoiceId: result.invoiceId },
          'Invoice processed successfully',
        );
        return result;
      } catch (error: unknown) {
        logger.warn(
          {
            jobId: job.id,
            customerId: job.data.customerId,
            attempt,
            err: error,
          },
          'Invoice processing failed; BullMQ will handle retries',
        );
        throw error;
      }
    }
  }
};

export const createTaskWorker = (): Worker<TaskPayload, TaskResult, TaskType> => {
  const worker = new Worker<TaskPayload, TaskResult, TaskType>(
    TASK_QUEUE_NAME,
    handleTask,
    {
      connection: redis,
      concurrency: Number.parseInt(process.env.WORKER_CONCURRENCY ?? '10', 10),
    },
  );

  worker.on('completed', (job, result) => {
    logger.info(
      {
        jobId: job.id,
        taskType: job.data.type,
        attempt: job.attemptsMade + 1,
        maxAttempts: job.opts.attempts ?? 1,
        result,
      },
      'Task completed',
    );
  });

  worker.on('failed', (job, error) => {
    logger.error(
      {
        jobId: job?.id,
        taskType: job?.data.type,
        attempt: job?.attemptsMade,
        maxAttempts: job?.opts.attempts ?? 1,
        err: error,
      },
      'Task failed',
    );
  });

  worker.on('stalled', (jobId) => {
    logger.warn({ jobId }, 'Task stalled and will be retried');
  });

  worker.on('error', (error) => {
    logger.error({ err: error }, 'Task worker error');
  });

  return worker;
};
