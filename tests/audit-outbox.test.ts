import { Redis } from 'ioredis';
import pino from 'pino';
import { afterAll, expect, it, vi } from 'vitest';
import { loadConfig } from '../src/config/env.js';
import { createAuditOutbox } from '../src/observability/audit-outbox.js';
import type { AuditRecord } from '../src/observability/audit.js';

const mocks = vi.hoisted(() => ({
  add: vi.fn(),
  retry: vi.fn(),
  failed: vi.fn(),
  jobs: vi.fn(),
  processor: undefined as ((job: { data: unknown }) => Promise<void>) | undefined,
}));
vi.mock('bullmq', () => ({
  Queue: class {
    add = mocks.add;
    getFailed = mocks.failed;
    getJobs = mocks.jobs;
    getJobCounts = vi.fn().mockResolvedValue({ waiting: 1, active: 0, delayed: 0, failed: 1 });
    on = vi.fn();
    waitUntilReady = vi.fn().mockResolvedValue(undefined);
    close = vi.fn().mockResolvedValue(undefined);
  },
  Worker: class {
    constructor(_name: string, processor: (job: { data: unknown }) => Promise<void>) {
      mocks.processor = processor;
    }
    on = vi.fn();
    waitUntilReady = vi.fn().mockResolvedValue(undefined);
    close = vi.fn().mockResolvedValue(undefined);
  },
}));
vi.mock('../src/observability/metrics.js', () => ({
  auditDeliveryTotal: { inc: vi.fn() },
  auditOutboxJobs: { set: vi.fn() },
  auditOutboxOldestAge: { set: vi.fn() },
  maintenanceLastSuccess: { set: vi.fn() },
}));
const client = new Redis({ lazyConnect: true });
afterAll(() => {
  client.disconnect();
});
const record: AuditRecord = {
  timestamp: new Date().toISOString(),
  action: 'dead_letter.list',
  outcome: 'success',
  actor: 'operator',
  requestId: 'request',
  method: 'GET',
  path: '/admin/api/dead-letter',
};
it('persists sanitized stable IDs, propagates archive failures for retries, and recovers exhausted jobs', async () => {
  mocks.add.mockResolvedValue(undefined);
  mocks.failed.mockResolvedValue([{ retry: mocks.retry }]);
  mocks.jobs.mockResolvedValue([{ timestamp: Date.now() - 1000, retry: mocks.retry }]);
  mocks.retry.mockResolvedValue(undefined);
  const archive = { deliver: vi.fn().mockResolvedValue(undefined), write: vi.fn(), close: vi.fn() };
  const logger = pino({ level: 'silent' });
  const errors = vi.spyOn(logger, 'error');
  const sink = createAuditOutbox(
    loadConfig({
      AUDIT_WORM_ENABLED: 'true',
      AUDIT_WORM_DELIVERY: 'outbox',
      AUDIT_WORM_ACCESS_KEY_ID: 'writer',
      AUDIT_WORM_SECRET_ACCESS_KEY: 'secret',
      MAINTENANCE_INTERVAL_MS: '300000',
    }),
    client,
    client,
    archive,
    logger,
  );
  await sink.ready();
  expect(mocks.retry).toHaveBeenCalledWith('failed', { resetAttemptsMade: true });
  await sink.write({ ...record, reason: 'test' });
  const data: AuditRecord = mocks.add.mock.calls[0][1];
  expect(data.archiveId).toMatch(/^[a-f0-9-]{36}$/u);
  expect(mocks.add.mock.calls[0][2]).toEqual({ jobId: data.archiveId });
  await mocks.processor?.({ data });
  archive.deliver.mockRejectedValueOnce(new Error('destination down'));
  await expect(mocks.processor?.({ data })).rejects.toThrow('destination down');
  await expect(mocks.processor?.({ data: { invalid: true } })).rejects.toThrow();
  mocks.add.mockRejectedValueOnce(new Error('Redis down'));
  await expect(sink.write(record)).resolves.toBeUndefined();
  expect(errors).toHaveBeenCalled();
  await sink.close();
  await sink.write(record);
  expect(archive.close).toHaveBeenCalled();
});
it('requires explicitly configured WORM for outbox, but preserves default direct mode', () => {
  expect(loadConfig({}).AUDIT_WORM_DELIVERY).toBe('direct');
  expect(() => loadConfig({ AUDIT_WORM_DELIVERY: 'outbox' })).toThrow('outbox requires');
});
