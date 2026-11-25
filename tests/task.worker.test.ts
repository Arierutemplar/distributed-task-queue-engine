import { EventEmitter } from 'node:events';
import type * as BullMQ from 'bullmq';
import { describe, expect, it, vi } from 'vitest';
import type { TaskJob } from '../src/workers/task.processor.js';

const mocks = vi.hoisted(() => ({
  add: vi.fn(async () => true),
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
  processed: vi.fn(),
  duration: vi.fn(),
  endToEnd: vi.fn(),
  state: vi.fn(),
}));

vi.mock('bullmq', async (importOriginal) => {
  const actual = await importOriginal<typeof BullMQ>();
  return {
    ...actual,
    Worker: class extends EventEmitter {},
  };
});
vi.mock('../src/config/redis.js', () => ({ redis: {}, coordinationRedis: {} }));
vi.mock('../src/config/logger.js', () => ({ logger: mocks }));
vi.mock('../src/observability/metrics.js', () => ({
  tasksProcessedTotal: { inc: mocks.processed },
  taskDuration: { observe: mocks.duration },
  taskEndToEndDuration: { observe: mocks.endToEnd },
  setCircuitState: mocks.state,
}));
vi.mock('../src/queues/task.queue.js', () => ({ TASK_QUEUE_NAME: 'tasks' }));
vi.mock('../src/queues/dead-letter.queue.js', () => ({
  addDeadLetter: mocks.add,
}));

import { createTaskWorker, routeToDeadLetter } from '../src/workers/task.worker.js';

const job = {
  id: '42',
  name: 'PROCESS_DOCUMENT',
  data: { type: 'PROCESS_DOCUMENT', documentId: 'd', content: 'hello' },
  opts: {},
  attemptsMade: 1,
  processedOn: 1_000,
  timestamp: 500,
  finishedOn: 2_500,
  failedReason: '',
  getState: vi.fn(async () => 'failed'),
} as unknown as TaskJob;

describe('worker events', () => {
  it('records completed and failed attempts, routes terminal failures and logs worker events', async () => {
    const worker = createTaskWorker();
    worker.emit(
      'completed',
      job,
      {
        taskType: 'PROCESS_DOCUMENT',
        documentId: 'd',
        characterCount: 5,
        wordCount: 1,
        sha256: 'hash',
        completedAt: '2026-01-01T00:00:00.000Z',
      },
      'active',
    );
    worker.emit('failed', job, new Error('failure'), 'active');
    await vi.waitFor(() => {
      expect(mocks.add).toHaveBeenCalledOnce();
    });
    expect(mocks.processed).toHaveBeenCalledWith({ task_type: job.name, outcome: 'completed' });
    expect(mocks.processed).toHaveBeenCalledWith({ task_type: job.name, outcome: 'failed' });
    expect(mocks.duration).toHaveBeenCalledWith({ task_type: job.name, outcome: 'failed' }, 1.5);
    expect(mocks.endToEnd).toHaveBeenCalledWith({ task_type: job.name, outcome: 'completed' }, 2);
    expect(mocks.endToEnd).toHaveBeenCalledWith({ task_type: job.name, outcome: 'failed' }, 2);
    expect(mocks.add).toHaveBeenCalledWith(
      expect.objectContaining({ failedReason: 'failure', originalJobId: '42' }),
    );
    worker.emit('failed', undefined, new Error('no job'), 'active');
    worker.emit('stalled', '42', 'active');
    worker.emit('error', new Error('worker error'));
    expect(mocks.warn).toHaveBeenCalledWith({ jobId: '42' }, 'Task stalled and will be retried');
    expect(mocks.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.any(Error) }),
      'Task failed without job context',
    );
    expect(mocks.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.any(Error) }),
      'Task worker error',
    );
  });

  it('does not count a retry as a terminal end-to-end outcome', () => {
    const worker = createTaskWorker();
    const finishedOn = job.finishedOn;
    job.finishedOn = undefined;
    try {
      worker.emit('failed', job, new Error('retry'), 'active');
      expect(mocks.endToEnd).not.toHaveBeenCalled();
    } finally {
      job.finishedOn = finishedOn;
    }
  });

  it('logs DLQ write failures and ignores jobs without an id', async () => {
    mocks.add.mockRejectedValueOnce(new Error('redis down'));
    const worker = createTaskWorker();
    worker.emit('failed', job, new Error('failure'), 'active');
    await vi.waitFor(() => {
      expect(mocks.error).toHaveBeenCalledWith(
        expect.objectContaining({ jobId: '42' }),
        'Failed to move task to the dead-letter queue',
      );
    });
    const id = job.id;
    job.id = undefined;
    try {
      await expect(routeToDeadLetter(job, new Error('x'))).resolves.toBe(false);
    } finally {
      job.id = id;
    }
  });
});
