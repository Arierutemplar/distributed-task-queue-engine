import { Queue, Worker } from 'bullmq';
import { afterAll, beforeEach, expect, it, vi } from 'vitest';
import { loadConfig } from '../../src/config/env.js';
import { logger } from '../../src/config/logger.js';
import { redis, coordinationRedis } from '../../src/config/redis.js';
import { createAuditOutbox } from '../../src/observability/audit-outbox.js';
import { createDlqReconciler } from '../../src/operations/dlq-reconciler.js';
import {
  captureRestoreSamples,
  verifyRestoreSamples,
} from '../../src/operations/restore-samples.js';
import { taskQueue } from '../../src/queues/task.queue.js';
import { deadLetterQueue, deadLetterJobId } from '../../src/queues/dead-letter.queue.js';
import { AUDIT_STREAM_KEY, closeConnections, resetState, waitFor } from './helpers.js';
import { SERVICE_NAME } from './constants.js';

beforeEach(resetState);
afterAll(closeConnections);
it('detects changed or missing sampled job and audit content', async () => {
  const job = await taskQueue.add('PROCESS_DOCUMENT', {
    type: 'PROCESS_DOCUMENT',
    documentId: 'representative',
    content: 'original',
  });
  if (!job.id) throw new Error('Expected job ID');
  const auditId = await redis.xadd(AUDIT_STREAM_KEY, '*', 'record', '{"action":"test"}');
  if (!auditId) throw new Error('Expected audit ID');
  const manifest = await captureRestoreSamples(redis, SERVICE_NAME, {
    jobs: [{ queue: 'tasks', id: job.id }],
    auditId,
  });
  await expect(verifyRestoreSamples(redis, SERVICE_NAME, manifest)).resolves.toBeUndefined();
  await job.updateData({
    type: 'PROCESS_DOCUMENT',
    documentId: 'representative',
    content: 'changed',
  });
  await expect(verifyRestoreSamples(redis, SERVICE_NAME, manifest)).rejects.toThrow(
    /does not match/u,
  );
  await redis.xdel(AUDIT_STREAM_KEY, auditId);
  await expect(verifyRestoreSamples(redis, SERVICE_NAME, manifest)).rejects.toThrow(
    /Missing sampled audit/u,
  );
});
it('recovers an exhausted persisted audit after process restart and archive recovery', async () => {
  const settings = loadConfig({
    ...process.env,
    AUDIT_WORM_ENABLED: 'true',
    AUDIT_WORM_DELIVERY: 'outbox',
    AUDIT_WORM_ACCESS_KEY_ID: 'test',
    AUDIT_WORM_SECRET_ACCESS_KEY: 'test',
    AUDIT_OUTBOX_ATTEMPTS: '1',
    MAINTENANCE_INTERVAL_MS: '300000',
  });
  let available = false;
  const delivered: string[] = [];
  const archive = {
    deliver: vi.fn(async (record: { archiveId?: string }) => {
      if (!available) throw new Error('S3 unavailable');
      delivered.push(record.archiveId!);
    }),
    write: vi.fn(),
    close: vi.fn(),
  };
  const queue = new Queue('audit-worm-outbox', {
    connection: redis,
    prefix: `${settings.SERVICE_NAME}:bull`,
  });
  let sink = createAuditOutbox(settings, coordinationRedis, redis, archive, logger);
  try {
    await sink.ready();
    await sink.write({
      timestamp: new Date().toISOString(),
      action: 'dead_letter.list',
      outcome: 'success',
      actor: 'admin',
      requestId: 'persisted',
      method: 'GET',
      path: '/admin/api/dead-letter',
    });
    await waitFor(async () => (await queue.getFailedCount()) === 1, 'durable failed audit');
    const [failed] = await queue.getFailed();
    const id = failed.id;
    await sink.close();
    available = true;
    sink = createAuditOutbox(settings, coordinationRedis, redis, archive, logger);
    await sink.ready();
    await waitFor(async () => delivered.includes(id!), 'recovered archive');
    expect(delivered).toEqual([id]);
    await waitFor(
      async () => (await queue.getJobCounts('waiting', 'active', 'failed')).failed === 0,
      'drained outbox',
    );
  } finally {
    await sink.close();
    await queue.close();
  }
});
it('repairs a terminal failure without the worker callback, once, and respects operator deletion', async () => {
  const worker = new Worker(
    'tasks',
    async () => {
      throw new Error('callback intentionally absent');
    },
    { connection: redis },
  );
  const loop = createDlqReconciler(
    loadConfig({ ...process.env, DLQ_RECONCILE_ENABLED: 'true' }),
    redis,
    logger,
  )!;
  try {
    const job = await taskQueue.add(
      'PROCESS_DOCUMENT',
      { type: 'PROCESS_DOCUMENT', documentId: 'repair', content: 'test' },
      { attempts: 1, removeOnFail: false },
    );
    await waitFor(async () => (await job.getState()) === 'failed', 'unrouted terminal failure');
    const id = deadLetterJobId(job.id!);
    expect(await deadLetterQueue.getJob(id)).toBeUndefined();
    await loop.run();
    const dead = await deadLetterQueue.getJob(id);
    expect(dead).toBeDefined();
    await loop.run();
    expect(await deadLetterQueue.getWaitingCount()).toBe(1);
    await dead!.remove();
    await loop.run();
    expect(await deadLetterQueue.getWaitingCount()).toBe(0);
  } finally {
    await loop.close();
    await worker.close();
  }
});
