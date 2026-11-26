import { Writable } from 'node:stream';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { mkdtemp, rmdir, unlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import pino from 'pino';
import request from 'supertest';
import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { config, loadConfig } from '../../src/config/env.js';
import { redis } from '../../src/config/redis.js';
import { deadLetterQueue } from '../../src/queues/dead-letter.queue.js';
import { taskPayloadSchema } from '../../src/types/task.types.js';
import { createAuditLog, type AuditRecord } from '../../src/observability/audit.js';
import { createAuditWormSink, type AuditS3Client } from '../../src/observability/audit-worm.js';
import {
  createBackupStorage,
  loadBackupSettings,
  rotateBackupStorage,
} from '../../src/operations/backup.js';
import {
  AUDIT_STREAM_KEY,
  adminAuthHeader,
  buildApp,
  closeConnections,
  documentPayload,
  resetState,
} from './helpers.js';

beforeEach(resetState);
afterAll(closeConnections);

const setup = async (enabled = true) => {
  const lines: string[] = [];
  const logger = pino(
    { level: 'info' },
    new Writable({
      write(chunk: Buffer, _encoding, callback) {
        lines.push(chunk.toString());
        callback();
      },
    }),
  );
  const client = {
    send: vi
      .fn<AuditS3Client['send']>()
      .mockResolvedValue({ $metadata: {}, VersionId: 'test-version' }),
    destroy: vi.fn(),
  };
  const settings = loadConfig({
    ...process.env,
    AUDIT_WORM_ENABLED: String(enabled),
    AUDIT_WORM_ACCESS_KEY_ID: 'fake-writer',
    AUDIT_WORM_SECRET_ACCESS_KEY: 'fake-secret',
    AUDIT_WORM_LOCK_MODE: 'COMPLIANCE',
    AUDIT_WORM_RETENTION_DAYS: '14',
  });
  const sink = createAuditWormSink(settings, logger, {
    client,
    now: () => new Date('2026-11-01T00:00:00.000Z'),
  });
  const audit = createAuditLog({
    logger,
    redis,
    streamKey: AUDIT_STREAM_KEY,
    sink,
  });
  const app = await buildApp({ auditLog: audit });
  return { app, audit, client, sink, lines };
};

describe('WORM export alongside real Redis and admin HTTP routes', () => {
  it('signs backup uploads with the newly rotated S3 credentials', async () => {
    const authorizations: string[] = [];
    const server = createServer((request, response) => {
      authorizations.push(request.headers.authorization ?? '');
      request.resume();
      request.on('end', () => {
        response.setHeader('x-amz-version-id', 'version-1');
        response.end();
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const directory = await mkdtemp(join(tmpdir(), 'dtq-backup-rotation-'));
    const file = join(directory, 'fixture');
    const body = Buffer.from('backup-rotation-fixture');
    await writeFile(file, body);
    const settings = loadBackupSettings({
      BACKUP_S3_ENDPOINT: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
      BACKUP_S3_BUCKET: 'test-backups',
      BACKUP_ENCRYPTION_KEY: 'aa'.repeat(32),
      BACKUP_S3_ACCESS_KEY_ID: 'old-backup-access',
      BACKUP_S3_SECRET_ACCESS_KEY: 'old-backup-secret',
    });
    let storage = createBackupStorage(settings);
    const checksum = createHash('sha256').update(body).digest('base64');
    const signal = new AbortController().signal;
    try {
      await storage.put('first', file, body.length, checksum, signal);
      storage = rotateBackupStorage(settings, storage, {
        ...settings,
        BACKUP_S3_ACCESS_KEY_ID: 'new-backup-access',
        BACKUP_S3_SECRET_ACCESS_KEY: 'new-backup-secret',
      }).storage;
      await storage.put('second', file, body.length, checksum, signal);
      expect(authorizations[0]).toContain('Credential=old-backup-access/');
      expect(authorizations[1]).toContain('Credential=new-backup-access/');
    } finally {
      storage.close();
      server.closeAllConnections();
      await new Promise<void>((resolve) => {
        server.close(() => {
          resolve();
        });
      });
      await unlink(file);
      await rmdir(directory);
    }
  });

  it('exports the same audit record with explicit lock and retention while keeping Redis/API behavior', async () => {
    const { app, audit, client, sink } = await setup();
    const response = await request(app)
      .get('/admin/api/dead-letter')
      .set('Authorization', adminAuthHeader);
    expect(response.status).toBe(200);
    expect(client.send).toHaveBeenCalledOnce();
    const object = client.send.mock.calls[0][0].input;
    expect(object).toMatchObject({
      ObjectLockMode: 'COMPLIANCE',
      ObjectLockRetainUntilDate: new Date('2026-11-15T00:00:00.000Z'),
      ChecksumAlgorithm: 'SHA256',
    });
    if (typeof object.Body !== 'string') throw new Error('Expected a JSON audit object');
    const archived = JSON.parse(object.Body) as AuditRecord;
    expect(archived).toMatchObject({
      action: 'dead_letter.list',
      subject: config.ADMIN_USERNAME,
      role: 'admin',
      actor: config.ADMIN_USERNAME,
      requestId: response.headers['x-request-id'],
    });
    expect(await audit.list(10)).toEqual([expect.objectContaining(archived)]);
    expect(
      (await request(app).get('/admin/api/audit').set('Authorization', adminAuthHeader)).status,
    ).toBe(200);
    await sink?.close();
  });

  it('keeps the admin API and Redis audit functional when S3 rejects writes', async () => {
    const { app, audit, client, sink, lines } = await setup();
    client.send.mockRejectedValue(new Error('S3 rejected fake-secret credentials'));
    const response = await request(app)
      .get('/admin/api/dead-letter')
      .set('Authorization', adminAuthHeader);
    expect(response.status).toBe(200);
    expect(await audit.list(10)).toEqual([
      expect.objectContaining({
        action: 'dead_letter.list',
        outcome: 'success',
        actor: config.ADMIN_USERNAME,
      }),
    ]);
    expect(lines.join('')).toContain('WORM audit write failed');
    expect(lines.join('')).not.toContain('fake-secret');
    expect(
      (await request(app).get('/admin/api/audit').set('Authorization', adminAuthHeader)).status,
    ).toBe(200);
    for (const id of ['worm-replay', 'worm-delete']) {
      await deadLetterQueue.add(
        'dead-letter',
        {
          originalJobId: id,
          taskType: 'PROCESS_DOCUMENT',
          data: taskPayloadSchema.parse(documentPayload),
          failedReason: 'test failure',
          attemptsMade: 3,
          failedAt: new Date().toISOString(),
        },
        { jobId: `dlq-${id}` },
      );
    }
    expect(
      (
        await request(app)
          .post('/admin/api/dead-letter/worm-replay/replay')
          .set('Authorization', adminAuthHeader)
      ).status,
    ).toBe(202);
    expect(
      (
        await request(app)
          .delete('/admin/api/dead-letter/worm-delete')
          .set('Authorization', adminAuthHeader)
      ).status,
    ).toBe(204);
    expect(await deadLetterQueue.getWaitingCount()).toBe(0);
    await sink?.close();
  });

  it('does not use S3 when disabled and still persists the admin audit', async () => {
    const { app, audit, client, sink } = await setup(false);
    expect(sink).toBeUndefined();
    expect(
      (await request(app).get('/admin/api/dead-letter').set('Authorization', adminAuthHeader))
        .status,
    ).toBe(200);
    expect(client.send).not.toHaveBeenCalled();
    expect(await audit.list(10)).toEqual([expect.objectContaining({ action: 'dead_letter.list' })]);
  });
});
