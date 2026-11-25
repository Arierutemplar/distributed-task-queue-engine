import { describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  eval: vi.fn(),
  hgetall: vi.fn(),
  expire: vi.fn(),
}));

vi.mock('bullmq', () => ({
  Queue: class {
    constructor(readonly name: string) {}
    toKey(key: string): string {
      return `bull:${this.name}:${key}`;
    }
  },
}));
vi.mock('../src/config/redis.js', () => ({ redis: mocks }));
vi.mock('../src/queues/task.queue.js', () => ({
  taskQueue: { toKey: (key: string) => `bull:tasks:${key}` },
}));

import { addDeadLetter, readDeadLetterCounts } from '../src/queues/dead-letter.queue.js';
import type { DeadLetterJobData } from '../src/types/task.types.js';

const failure: DeadLetterJobData = {
  originalJobId: '42',
  taskType: 'PROCESS_DOCUMENT',
  data: { type: 'PROCESS_DOCUMENT', documentId: 'doc', content: 'text' },
  failedReason: 'failed',
  attemptsMade: 1,
  failedAt: '2026-01-01T00:00:00.000Z',
};

describe('atomic dead-letter routing', () => {
  it('distinguishes a new entry from a duplicate and rejects invalid Redis replies', async () => {
    mocks.eval.mockResolvedValueOnce(1).mockResolvedValueOnce(0).mockResolvedValueOnce('invalid');
    await expect(addDeadLetter(failure)).resolves.toBe(true);
    await expect(addDeadLetter(failure)).resolves.toBe(false);
    await expect(addDeadLetter(failure)).rejects.toThrow('Invalid dead-letter routing reply');
  });

  it('propagates Redis errors instead of claiming the entry was written', async () => {
    mocks.eval.mockRejectedValueOnce(new Error('Redis unavailable'));
    await expect(addDeadLetter(failure)).rejects.toThrow('Redis unavailable');
  });

  it('refreshes the per-process counter only when it exists', async () => {
    mocks.hgetall.mockResolvedValueOnce({ PROCESS_DOCUMENT: '1' }).mockResolvedValueOnce({});
    await expect(readDeadLetterCounts()).resolves.toEqual({ PROCESS_DOCUMENT: '1' });
    expect(mocks.expire).toHaveBeenCalledOnce();
    await expect(readDeadLetterCounts()).resolves.toEqual({});
    expect(mocks.expire).toHaveBeenCalledOnce();
  });
});
