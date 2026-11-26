import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createHash } from 'node:crypto';
import type { Express } from 'express';
import { exportJWK, generateKeyPair, jwtDecrypt, SignJWT } from 'jose';
import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createApp } from '../../src/app.js';
import { loadConfig } from '../../src/config/env.js';
import { createSecretsReloader } from '../../src/config/secrets-reload.js';
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
let exchange: { access_token: string; id_token: string };
let exchangeBody: URLSearchParams;
const audit = createIntegrationAuditLog();

beforeAll(async () => {
  pair = await generateKeyPair('RS256');
  const key = await exportJWK(pair.publicKey);
  server = createServer(async (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    if (req.url?.endsWith('/certs')) {
      res.end(JSON.stringify({ keys: [{ ...key, kid: 'integration', alg: 'RS256' }] }));
      return;
    }
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    exchangeBody = new URLSearchParams(Buffer.concat(chunks).toString());
    res.end(JSON.stringify(exchange));
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
  it('uses the rotated OIDC client secret and invalidates sessions on session-key rotation', async () => {
    const oldSessionSecret = 'old-integration-session-secret-0123456789';
    const newSessionSecret = 'new-integration-session-secret-0123456789';
    const source = {
      ...process.env,
      ADMIN_AUTH_MODE: 'oidc',
      OIDC_ISSUER_URL: issuer,
      OIDC_PUBLIC_ISSUER_URL: issuer,
      OIDC_REDIRECT_URI: 'http://localhost:3000/admin/callback',
      OIDC_CLIENT_SECRET: 'old-oidc-secret',
      SESSION_SECRET: oldSessionSecret,
    };
    const settings = loadConfig(source);
    const reloader = createSecretsReloader(settings.API_KEYS, source);
    const rotatedApp = await createApp({
      config: settings,
      secretsReloader: reloader,
      auditLog: audit,
    });
    const login = async (sessionSecret: string) => {
      const start = await request(rotatedApp).get('/admin/login');
      const cookie = (start.headers['set-cookie'] as unknown as string[])[0].split(';')[0];
      const encrypted = cookie.slice(cookie.indexOf('=') + 1);
      const transaction = (
        await jwtDecrypt(encrypted, createHash('sha256').update(sessionSecret).digest())
      ).payload;
      exchange = {
        access_token: await token('admin'),
        id_token: await new SignJWT({ nonce: transaction.nonce })
          .setProtectedHeader({ alg: 'RS256', kid: 'integration' })
          .setIssuer(issuer)
          .setAudience('task-queue-admin')
          .setSubject('subject-admin')
          .setIssuedAt()
          .setExpirationTime('5m')
          .sign(pair.privateKey),
      };
      return request(rotatedApp)
        .get('/admin/callback')
        .query({ state: transaction.state, code: 'code' })
        .set('Cookie', cookie);
    };
    const before = await login(oldSessionSecret);
    expect(before.status).toBe(302);
    expect(exchangeBody.get('client_secret')).toBe('old-oidc-secret');
    const session = (before.headers['set-cookie'] as unknown as string[])
      .find((value) => value.startsWith('admin_session='))
      ?.split(';')[0];
    expect(session).toBeDefined();
    source.OIDC_CLIENT_SECRET = 'new-oidc-secret';
    source.SESSION_SECRET = newSessionSecret;
    const result = await request(rotatedApp)
      .post('/admin/api/secrets/reload')
      .auth(await token('admin'), { type: 'bearer' });
    expect(result.status).toBe(200);
    expect(result.body.applied).toEqual(
      expect.arrayContaining(['OIDC_CLIENT_SECRET', 'SESSION_SECRET']),
    );
    expect((await request(rotatedApp).get('/admin/api/audit').set('Cookie', session!)).status).toBe(
      401,
    );
    const after = await login(newSessionSecret);
    expect(after.status).toBe(302);
    expect(exchangeBody.get('client_secret')).toBe('new-oidc-secret');
  });

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
