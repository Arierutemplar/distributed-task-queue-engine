import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Express } from 'express';
import { exportJWK, generateKeyPair, SignJWT } from 'jose';
import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createApp } from '../../src/app.js';
import { loadConfig } from '../../src/config/env.js';
import { deadLetterQueue } from '../../src/queues/dead-letter.queue.js';
import { taskQueue } from '../../src/queues/task.queue.js';
import { taskPayloadSchema } from '../../src/types/task.types.js';
import {
  closeConnections,
  createIntegrationAuditLog,
  documentPayload,
  resetState,
} from './helpers.js';

let app: Express;
let server: Server;
let issuer: string;
let pair: Awaited<ReturnType<typeof generateKeyPair>>;
const audit = createIntegrationAuditLog();

beforeAll(async () => {
  pair = await generateKeyPair('RS256');
  const key = await exportJWK(pair.publicKey);
  server = createServer((_req, res) => {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ keys: [{ ...key, kid: 'integration', alg: 'RS256' }] }));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  issuer = `http://127.0.0.1:${(server.address() as AddressInfo).port}/realms/task-queue`;
  app = await createApp({
    config: loadConfig({
      ...process.env,
      ADMIN_AUTH_MODE: 'oidc',
      OIDC_ISSUER_URL: issuer,
      OIDC_PUBLIC_ISSUER_URL: issuer,
      OIDC_REDIRECT_URI: 'http://localhost:3000/admin/callback',
      SESSION_SECRET: 'integration-only-session-secret-0123456789',
    }),
    auditLog: audit,
  });
});
beforeEach(resetState);
afterAll(async () => {
  await closeConnections();
  await new Promise<void>((resolve, reject) =>
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    }),
  );
});

const token = (role: string, expiration = '5m', signingKey = pair.privateKey) =>
  new SignJWT({
    preferred_username: `local-${role}`,
    resource_access: { 'task-queue-admin': { roles: [role] } },
  })
    .setProtectedHeader({ alg: 'RS256', kid: 'integration' })
    .setIssuer(issuer)
    .setAudience('task-queue-admin')
    .setSubject(`subject-${role}`)
    .setIssuedAt()
    .setExpirationTime(expiration)
    .sign(signingKey);

const seed = (id: string) =>
  deadLetterQueue.add(
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

describe('OIDC RBAC on real administrative routes', () => {
  it('allows viewer reads, denies writes before touching DLQ and persists identity in audit', async () => {
    await seed('viewer-job');
    const viewer = await token('viewer');
    for (const path of ['/admin/queues', '/admin/api/dead-letter', '/admin/api/audit']) {
      expect((await request(app).get(path).auth(viewer, { type: 'bearer' })).status).toBe(200);
    }
    for (const [method, path] of [
      ['post', '/admin/api/dead-letter/viewer-job/replay'],
      ['delete', '/admin/api/dead-letter/viewer-job'],
      ['put', '/admin/queues/api/queues/tasks/pause'],
      ['patch', '/admin/unknown'],
      ['post', '/admin/api/secrets/reload'],
    ] as const) {
      expect((await request(app)[method](path).auth(viewer, { type: 'bearer' })).status).toBe(403);
    }
    expect(await deadLetterQueue.getJob('dlq-viewer-job')).toBeDefined();
    expect(await taskQueue.getWaitingCount()).toBe(0);
    const records = await audit.list(100);
    expect(records.filter((event) => event.action === 'admin.access_denied')).toEqual(
      Array.from({ length: 5 }, () =>
        expect.objectContaining({
          outcome: 'denied',
          actor: 'local-viewer',
          subject: 'subject-viewer',
          role: 'viewer',
          statusCode: 403,
        }),
      ),
    );
    expect(records.find((event) => event.action === 'dead_letter.list')).toMatchObject({
      role: 'viewer',
      subject: 'subject-viewer',
    });
    expect(records.some((event) => event.action === 'admin.bull_board_action')).toBe(false);
  });

  it('allows admin reads, replay, delete and mutable Bull Board actions', async () => {
    await seed('admin-replay');
    await seed('admin-delete');
    const admin = await token('admin');
    for (const path of ['/admin/queues', '/admin/api/dead-letter', '/admin/api/audit']) {
      expect((await request(app).get(path).auth(admin, { type: 'bearer' })).status).toBe(200);
    }
    const replay = await request(app)
      .post('/admin/api/dead-letter/admin-replay/replay')
      .auth(admin, { type: 'bearer' });
    expect(replay.status).toBe(202);
    expect(await taskQueue.getJob(replay.body.taskId)).toBeDefined();
    expect(
      (
        await request(app)
          .delete('/admin/api/dead-letter/admin-delete')
          .auth(admin, { type: 'bearer' })
      ).status,
    ).toBe(204);
    expect(await deadLetterQueue.getWaitingCount()).toBe(0);
    expect(
      (
        await request(app)
          .put('/admin/queues/api/queues/tasks/pause')
          .auth(admin, { type: 'bearer' })
      ).status,
    ).toBe(200);
    expect(await taskQueue.isPaused()).toBe(true);
    const records = await audit.list(100);
    expect(records.find((event) => event.action === 'dead_letter.replay')).toMatchObject({
      actor: 'local-admin',
      subject: 'subject-admin',
      role: 'admin',
      outcome: 'success',
    });
  });

  it('returns 401 for malformed, expired and incorrect-signature tokens without external Keycloak', async () => {
    const other = await generateKeyPair('RS256');
    for (const value of [
      'invalid',
      await token('admin', '-1s'),
      await token('admin', '5m', other.privateKey),
    ]) {
      expect(
        (await request(app).get('/admin/api/dead-letter').auth(value, { type: 'bearer' })).status,
      ).toBe(401);
    }
  });

  it('rejects a validly signed token without an administrative role and records the denial', async () => {
    const unprivileged = await token('other');
    const response = await request(app)
      .get('/admin/api/dead-letter?secret=do-not-log')
      .auth(unprivileged, { type: 'bearer' });
    expect(response.status).toBe(403);
    expect(await audit.list(20)).toContainEqual(
      expect.objectContaining({
        action: 'admin.access_denied',
        outcome: 'denied',
        path: '/admin/api/dead-letter',
        statusCode: 403,
      }),
    );
    expect(JSON.stringify(await audit.list(20))).not.toContain(unprivileged);
    expect(JSON.stringify(await audit.list(20))).not.toContain('do-not-log');
  });
});
