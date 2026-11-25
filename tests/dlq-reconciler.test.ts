import { Redis } from 'ioredis';
import pino from 'pino';
import { afterAll, expect, it, vi } from 'vitest';
import { loadConfig } from '../src/config/env.js';
import { createDlqReconciler } from '../src/operations/dlq-reconciler.js';
const mocks = vi.hoisted(() => ({ getJob: vi.fn(), route: vi.fn() }));
vi.mock('../src/queues/task.queue.js', () => ({
  taskQueue: { toKey: (key: string) => `bull:tasks:${key}`, getJob: mocks.getJob },
}));
vi.mock('../src/queues/dead-letter.queue.js', () => ({ addDeadLetter: mocks.route }));
vi.mock('../src/observability/metrics.js', () => ({
  dlqReconciledTotal: { inc: vi.fn() },
  maintenanceLastSuccess: { set: vi.fn() },
}));
const client = new Redis({ lazyConnect: true });
const logger = pino({ level: 'silent' });
afterAll(() => {
  client.disconnect();
});
it('is disabled by default and preserves pending batches after a routing failure', async () => {
  expect(createDlqReconciler(loadConfig({}), client, logger)).toBeUndefined();
  const scan = vi.spyOn(client, 'zscan').mockResolvedValue(['12', ['1', '1', '2', '2', '3', '3']]);
  mocks.getJob.mockImplementation(async (id: string) => ({
    id,
    name: 'PROCESS_DOCUMENT',
    data: { type: 'PROCESS_DOCUMENT' },
    finishedOn: Date.now(),
    failedReason: 'failure',
    attemptsMade: 3,
  }));
  mocks.route.mockRejectedValueOnce(new Error('Redis unavailable')).mockResolvedValue(true);
  const loop = createDlqReconciler(
    loadConfig({ DLQ_RECONCILE_ENABLED: 'true', MAINTENANCE_BATCH_SIZE: '1' }),
    client,
    logger,
  )!;
  await loop.run();
  await loop.run();
  expect(mocks.route.mock.calls[0][0].originalJobId).toBe('1');
  expect(mocks.route.mock.calls[1][0].originalJobId).toBe('1');
  await loop.run();
  await loop.run();
  expect(scan).toHaveBeenCalledTimes(1);
  await loop.run();
  expect(scan.mock.calls[1][1]).toBe('12');
  await loop.close();
});
it('skips deleted/nonterminal jobs and does not claim already-routed failures as new', async () => {
  vi.spyOn(client, 'zscan').mockResolvedValue([
    '0',
    ['missing', '1', 'active', '2', 'routed', '3'],
  ]);
  mocks.getJob
    .mockResolvedValueOnce(undefined)
    .mockResolvedValueOnce({ id: 'active' })
    .mockResolvedValueOnce({
      id: 'routed',
      name: 'PROCESS_DOCUMENT',
      data: {},
      finishedOn: Date.now(),
      failedReason: 'failed',
      attemptsMade: 1,
    });
  mocks.route.mockResolvedValue(false);
  const loop = createDlqReconciler(loadConfig({ DLQ_RECONCILE_ENABLED: 'true' }), client, logger)!;
  await loop.run();
  expect(mocks.route).toHaveBeenCalledTimes(1);
  await loop.close();
});
