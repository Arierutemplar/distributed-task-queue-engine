import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Writable } from 'node:stream';
import pino from 'pino';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { loadConfig } from '../src/config/env.js';
import { createAuditWormSink, type AuditS3Client } from '../src/observability/audit-worm.js';
import { createAuditLog, type AuditRecord } from '../src/observability/audit.js';

const record: AuditRecord = {
  timestamp: '2026-01-01T00:00:00.000Z',
  action: 'dead_letter.list',
  outcome: 'success',
  actor: 'operator',
  subject: 'subject-1',
  role: 'viewer',
  requestId: 'req-1',
  method: 'GET',
  path: '/admin/api/dead-letter',
  statusCode: 200,
};

const setup = (overrides: NodeJS.ProcessEnv = {}) => {
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
    send: vi.fn<AuditS3Client['send']>().mockResolvedValue({
      $metadata: {},
      VersionId: 'immutable-version',
    }),
    destroy: vi.fn(),
  };
  const settings = loadConfig({
    AUDIT_WORM_ENABLED: 'true',
    AUDIT_WORM_BUCKET: 'audit-test',
    AUDIT_WORM_ACCESS_KEY_ID: 'test-writer',
    AUDIT_WORM_SECRET_ACCESS_KEY: 'test-secret',
    ...overrides,
  });
  const sink = createAuditWormSink(settings, logger, {
    client,
    now: () => new Date('2026-02-01T12:00:00.000Z'),
  });
  return { sink, client, logger, lines };
};

afterEach(() => {
  vi.useRealTimers();
});

describe('WORM audit sink', () => {
  it('uses rotated S3 credentials for subsequent archive requests', async () => {
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
    const port = (server.address() as AddressInfo).port;
    const settings = loadConfig({
      AUDIT_WORM_ENABLED: 'true',
      AUDIT_WORM_ENDPOINT: `http://127.0.0.1:${port}`,
      AUDIT_WORM_ACCESS_KEY_ID: 'first-access',
      AUDIT_WORM_SECRET_ACCESS_KEY: 'first-secret',
    });
    const sink = createAuditWormSink(settings, pino({ level: 'silent' }));
    try {
      await sink?.deliver(record);
      sink?.updateCredentials('next-access', 'next-secret');
      await sink?.deliver(record);
      expect(authorizations[0]).toContain('Credential=first-access/');
      expect(authorizations[1]).toContain('Credential=next-access/');
    } finally {
      await sink?.close();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => {
          if (error) reject(error);
          else resolve();
        }),
      );
    }
  });

  it('lets an in-flight archive complete before closing the replaced client', async () => {
    const { sink, client } = setup();
    let finish: ((value: { $metadata: object; VersionId: string }) => void) | undefined;
    client.send.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const operation = sink?.deliver(record);
    sink?.updateCredentials('next-access', 'next-secret');
    expect(client.destroy).not.toHaveBeenCalled();
    if (!finish) throw new Error('Archive request was not started');
    finish({ $metadata: {}, VersionId: 'version' });
    await operation;
    expect(client.destroy).toHaveBeenCalledOnce();
    await sink?.close();
    expect(client.destroy).toHaveBeenCalledOnce();
  });

  it.each(['GOVERNANCE', 'COMPLIANCE'])(
    'writes unique, checksummed objects with %s retention',
    async (mode) => {
      const { sink, client, lines } = setup({
        AUDIT_WORM_LOCK_MODE: mode,
        AUDIT_WORM_RETENTION_DAYS: '7',
      });
      expect(sink).toBeDefined();
      await sink?.write(record);
      await sink?.write(record);
      const [first, second] = client.send.mock.calls.map(([command]) => command.input);
      expect(first).toMatchObject({
        Bucket: 'audit-test',
        Body: JSON.stringify(record),
        ContentType: 'application/json',
        ChecksumAlgorithm: 'SHA256',
        ChecksumSHA256: createHash('sha256').update(JSON.stringify(record)).digest('base64'),
        ObjectLockMode: mode,
        ObjectLockRetainUntilDate: new Date('2026-02-08T12:00:00.000Z'),
      });
      expect(first.Key).toMatch(/^distributed-task-queue\/2026-01-01\/[a-f0-9-]+\.json$/u);
      expect(second.Key).not.toBe(first.Key);
      expect(lines.join('')).toContain('WORM audit write succeeded');
      expect(lines.join('')).not.toContain('test-secret');
      await sink?.close();
      expect(client.destroy).toHaveBeenCalledOnce();
    },
  );

  it('does not contact S3 when disabled or unconfigured, preserving logs-only auditing', async () => {
    for (const overrides of [
      { AUDIT_WORM_ENABLED: 'false' },
      { AUDIT_WORM_SECRET_ACCESS_KEY: '' },
      { AUDIT_WORM_ACCESS_KEY_ID: '' },
    ]) {
      const { sink, client, logger, lines } = setup(overrides);
      expect(sink).toBeUndefined();
      const audit = createAuditLog({ logger, sink });
      await audit.record(record);
      expect(client.send).not.toHaveBeenCalled();
      expect(lines.join('')).toContain('"action":"dead_letter.list"');
      if (overrides.AUDIT_WORM_ENABLED !== 'false') {
        expect(lines.join('')).toContain('credentials are incomplete');
      }
    }
  });

  it('archives in logs-only mode and logs safe failures without rejecting the audit flow', async () => {
    const { sink, client, logger, lines } = setup();
    const audit = createAuditLog({ logger, sink });
    await audit.record(record);
    client.send.mockRejectedValueOnce(
      new Error('Authorization test-secret at https://test-writer:test-secret@s3.test'),
    );
    await expect(audit.record(record)).resolves.toBeUndefined();
    expect(client.send).toHaveBeenCalledTimes(2);
    expect(lines.join('')).toContain('WORM audit write failed');
    expect(lines.join('')).not.toContain('test-secret');
    expect(lines.join('')).not.toContain('test-writer');
    await expect(audit.list(10)).resolves.toEqual([]);
    await sink?.close();
  });

  it('does not report success when the destination returns an unversioned object', async () => {
    const { sink, client, lines } = setup();
    client.send.mockResolvedValueOnce({ $metadata: {} });
    await expect(sink?.write(record)).resolves.toBeUndefined();
    expect(lines.join('')).toContain('WORM audit write failed');
    expect(lines.join('')).not.toContain('WORM audit write succeeded');
    await sink?.close();
  });

  it('bounds hung writes, aborts the client, and drains them before destroying the connection', async () => {
    vi.useFakeTimers();
    const { sink, client, lines } = setup({ AUDIT_WORM_TIMEOUT_MS: '100' });
    client.send.mockImplementationOnce(() => new Promise(() => undefined));
    const write = sink?.write(record);
    const close = sink?.close();
    expect(client.destroy).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(100);
    await expect(write).resolves.toBeUndefined();
    await expect(close).resolves.toBeUndefined();
    expect(client.send.mock.calls[0][1].abortSignal.aborted).toBe(true);
    expect(client.destroy).toHaveBeenCalledOnce();
    expect(lines.join('')).toContain('TaskTimeoutError');
    await sink?.write(record);
    expect(client.send).toHaveBeenCalledOnce();
    expect(lines.join('')).toContain('WORM archive is closed');
  });

  it('rejects unsafe endpoints and invalid retention configuration', () => {
    for (const env of [
      { AUDIT_WORM_ENDPOINT: 'http://external.example' },
      { AUDIT_WORM_ENDPOINT: 'https://user:secret@s3.example' },
      { AUDIT_WORM_RETENTION_DAYS: '0' },
      { AUDIT_WORM_LOCK_MODE: 'NONE' },
    ]) {
      expect(() => loadConfig(env)).toThrow();
    }
  });
});
