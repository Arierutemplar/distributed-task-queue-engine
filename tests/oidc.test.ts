import { createHash } from 'node:crypto';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import express from 'express';
import { EncryptJWT, exportJWK, generateKeyPair, jwtDecrypt, SignJWT, type JWTPayload } from 'jose';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config/env.js';
import { createAdminSecurity, resolveAdminRole } from '../src/middleware/oidc.middleware.js';

const secret = 'test-only-session-secret-0123456789abcdef';
let server: Server;
let issuer: string;
let pair: Awaited<ReturnType<typeof generateKeyPair>>;
let exchange: { access_token: string; id_token: string };
let exchangeBody: URLSearchParams;
let exchangeStatus = 200;

beforeAll(async () => {
  pair = await generateKeyPair('RS256');
  const publicKey = await exportJWK(pair.publicKey);
  server = createServer(async (req, res) => {
    if (req.url?.endsWith('/certs')) {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ keys: [{ ...publicKey, kid: 'test', alg: 'RS256' }] }));
      return;
    }
    const chunks = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    exchangeBody = new URLSearchParams(Buffer.concat(chunks).toString());
    res.writeHead(exchangeStatus, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(exchange));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  issuer = `http://127.0.0.1:${(server.address() as AddressInfo).port}/realms/test`;
});
afterAll(
  () =>
    new Promise<void>((resolve, reject) =>
      server.close((error) => {
        if (error) reject(error);
        else resolve();
      }),
    ),
);

const token = (claims: JWTPayload = {}, signingKey = pair.privateKey) =>
  new SignJWT({ realm_access: { roles: ['viewer'] }, preferred_username: 'reader', ...claims })
    .setProtectedHeader({ alg: 'RS256', kid: 'test' })
    .setIssuer(typeof claims.iss === 'string' ? claims.iss : issuer)
    .setAudience(claims.aud ?? 'task-queue-admin')
    .setSubject('user-123')
    .setIssuedAt()
    .setExpirationTime(claims.exp ?? '5m')
    .sign(signingKey);

const appFor = (overrides: Record<string, string> = {}) => {
  const settings = loadConfig({
    NODE_ENV: 'test',
    ADMIN_AUTH_MODE: 'oidc',
    OIDC_ISSUER_URL: issuer,
    OIDC_REDIRECT_URI: 'http://localhost:3000/admin/callback',
    SESSION_SECRET: secret,
    ...overrides,
  });
  const security = createAdminSecurity(settings);
  const app = express();
  app.use('/admin', security.router, security.authenticate);
  app.all('/admin/queues', (_req, res) => res.json({ ok: true }));
  app.all('/admin/api/dead-letter', (_req, res) => res.json({ ok: true }));
  return app;
};
const cookieHeaders = (response: request.Response): string[] => {
  const values = response.headers['set-cookie'];
  if (!Array.isArray(values)) throw new Error('Expected Set-Cookie headers');
  return values;
};
const cookies = (response: request.Response): string[] =>
  cookieHeaders(response).map((value) => value.split(';')[0]);
const transaction = async (response: request.Response) => {
  const encrypted = cookies(response)[0].split('=')[1];
  return (await jwtDecrypt(encrypted, createHash('sha256').update(secret).digest())).payload;
};

describe('OIDC JWT and roles', () => {
  it('validates configuration only when OIDC is active', () => {
    expect(loadConfig({}).ADMIN_AUTH_MODE).toBe('basic');
    expect(() => loadConfig({ ADMIN_AUTH_MODE: 'oidc' })).toThrow('SESSION_SECRET');
    expect(() => appFor({ OIDC_REDIRECT_URI: 'http://external.example/admin/callback' })).toThrow(
      'HTTPS',
    );
    expect(() => appFor({ OIDC_ISSUER_URL: 'ftp://localhost/realm' })).toThrow('HTTPS');
  });

  it('resolves realm and client roles, preferring admin and ignoring unrelated clients', () => {
    expect(resolveAdminRole({ realm_access: { roles: ['viewer', 'admin'] } }, 'client')).toBe(
      'admin',
    );
    expect(resolveAdminRole({ resource_access: { client: { roles: ['viewer'] } } }, 'client')).toBe(
      'viewer',
    );
    expect(
      resolveAdminRole({ resource_access: { other: { roles: ['admin'] } } }, 'client'),
    ).toBeUndefined();
    expect(resolveAdminRole({ realm_access: { roles: 'admin' } }, 'client')).toBeUndefined();
  });

  it('rejects missing, malformed, expired, wrong issuer/audience/signature and missing-exp tokens', async () => {
    const app = appFor();
    const other = await generateKeyPair('RS256');
    const noExpiration = await new SignJWT({})
      .setProtectedHeader({ alg: 'RS256', kid: 'test' })
      .setIssuer(issuer)
      .setAudience('task-queue-admin')
      .setSubject('user')
      .setIssuedAt()
      .sign(pair.privateKey);
    for (const value of [
      'not-a-jwt',
      await token({ exp: Math.floor(Date.now() / 1_000) - 1 }),
      await token({ iss: 'https://wrong.example' }),
      await token({ aud: 'wrong-client' }),
      await token({}, other.privateKey),
      noExpiration,
    ]) {
      expect(
        (await request(app).get('/admin/api/dead-letter').auth(value, { type: 'bearer' })).status,
      ).toBe(401);
    }
    expect((await request(app).get('/admin/api/dead-letter')).status).toBe(401);
    expect(
      (await request(app).get('/admin/queues').set('Authorization', 'Basic invalid')).status,
    ).toBe(401);
    expect(
      (
        await request(app)
          .get('/admin/api/dead-letter')
          .auth(await token({ realm_access: { roles: [] } }), { type: 'bearer' })
      ).status,
    ).toBe(403);
  });

  it('gates every mutating method and allows Bearer admins without cookie CSRF checks', async () => {
    const app = appFor();
    const viewer = await token();
    const admin = await token({ resource_access: { 'task-queue-admin': { roles: ['admin'] } } });
    for (const method of ['get', 'head', 'post', 'put', 'patch', 'delete'] as const) {
      expect(
        (await request(app)[method]('/admin/queues').auth(viewer, { type: 'bearer' })).status,
      ).toBe(['get', 'head'].includes(method) ? 200 : 403);
      expect(
        (await request(app)[method]('/admin/queues').auth(admin, { type: 'bearer' })).status,
      ).toBe(200);
    }
  });
});

describe('OIDC browser login', () => {
  it('uses PKCE/state/nonce, exchanges the code and enforces encrypted session and CSRF', async () => {
    const app = appFor({ OIDC_CLIENT_SECRET: 'test-only-confidential-secret' });
    expect((await request(app).get('/admin/queues')).headers.location).toBe('/admin/login');
    const login = await request(app).get('/admin/login');
    const url = new URL(login.headers.location);
    const data = await transaction(login);
    expect(url.searchParams.get('state')).toBe(data.state);
    expect(url.searchParams.get('nonce')).toBe(data.nonce);
    expect(url.searchParams.get('code_challenge_method')).toBe('S256');
    expect(url.searchParams.get('code_challenge')).toBe(
      createHash('sha256').update(String(data.verifier)).digest('base64url'),
    );
    exchange = {
      access_token: await token({ realm_access: { roles: ['admin'] } }),
      id_token: await token({ nonce: data.nonce }),
    };
    exchangeStatus = 200;
    const callback = await request(app)
      .get('/admin/callback')
      .query({ code: 'test-code', state: data.state })
      .set('Cookie', cookies(login));
    expect(callback.status).toBe(302);
    expect(callback.headers.location).toBe('/admin/queues');
    expect(exchangeBody.get('code_verifier')).toBe(data.verifier);
    expect(exchangeBody.get('client_secret')).toBe('test-only-confidential-secret');
    const session = cookies(callback).filter((value) => value.startsWith('admin_session='));
    expect(cookieHeaders(callback).join(' ')).toContain('HttpOnly');
    expect(cookieHeaders(callback).join(' ')).toContain('SameSite=Lax');
    expect(session[0]).not.toContain(exchange.access_token);
    expect((await request(app).get('/admin/queues').set('Cookie', session)).status).toBe(200);
    expect((await request(app).post('/admin/queues').set('Cookie', session)).status).toBe(403);
    expect(
      (
        await request(app)
          .post('/admin/queues')
          .set('Cookie', session)
          .set('Origin', 'http://localhost:3000')
      ).status,
    ).toBe(200);
    expect(
      (await request(app).get('/admin/queues').set('Cookie', 'admin_session=tampered')).status,
    ).toBe(401);
    expect(
      (await request(app).get('/admin/queues').set('Cookie', 'admin_session=a; admin_session=b'))
        .status,
    ).toBe(401);
    expect(
      (
        await request(app)
          .get('/admin/queues')
          .set('Cookie', session)
          .set('Authorization', 'Bearer invalid')
      ).status,
    ).toBe(401);
    const logout = await request(app).get('/admin/logout');
    expect(cookieHeaders(logout).join(' ')).toContain('admin_session=;');
    expect(logout.headers.location).toContain('/protocol/openid-connect/logout');
  });

  it('rejects absent transaction, state tampering, rejected code, and nonce mismatch', async () => {
    const app = appFor();
    expect((await request(app).get('/admin/callback?code=x&state=y')).status).toBe(401);
    const login = await request(app).get('/admin/login');
    const data = await transaction(login);
    expect(
      (await request(app).get('/admin/callback?code=x&state=wrong').set('Cookie', cookies(login)))
        .status,
    ).toBe(401);
    exchangeStatus = 400;
    expect(
      (
        await request(app)
          .get('/admin/callback')
          .query({ code: 'x', state: data.state })
          .set('Cookie', cookies(login))
      ).status,
    ).toBe(401);
    exchangeStatus = 200;
    exchange = { access_token: await token(), id_token: await token({ nonce: 'wrong' }) };
    expect(
      (
        await request(app)
          .get('/admin/callback')
          .query({ code: 'x', state: data.state })
          .set('Cookie', cookies(login))
      ).status,
    ).toBe(401);
    exchange = {
      access_token: await token({ realm_access: { roles: [] } }),
      id_token: await token({ nonce: data.nonce }),
    };
    expect(
      (
        await request(app)
          .get('/admin/callback')
          .query({ code: 'x', state: data.state })
          .set('Cookie', cookies(login))
      ).status,
    ).toBe(403);
  });

  it('sets Secure cookies for HTTPS redirects', async () => {
    const login = await request(
      appFor({ OIDC_REDIRECT_URI: 'https://app.example/admin/callback' }),
    ).get('/admin/login');
    expect(cookieHeaders(login).join(' ')).toContain('__Secure-admin_oidc=');
    expect(cookieHeaders(login).join(' ')).toContain('; Secure;');
  });

  it('keeps browser viewers read-only and rejects expired sessions or access tokens', async () => {
    const app = appFor();
    const login = await request(app).get('/admin/login');
    const data = await transaction(login);
    exchangeStatus = 200;
    exchange = { access_token: await token(), id_token: await token({ nonce: data.nonce }) };
    const callback = await request(app)
      .get('/admin/callback')
      .query({ code: 'viewer-code', state: data.state })
      .set('Cookie', cookies(login));
    expect(callback.status).toBe(302);
    const session = cookies(callback).filter((value) => value.startsWith('admin_session='));
    expect((await request(app).get('/admin/queues').set('Cookie', session)).status).toBe(200);
    expect(
      (
        await request(app)
          .delete('/admin/queues')
          .set('Cookie', session)
          .set('Origin', 'http://localhost:3000')
      ).status,
    ).toBe(403);

    const now = Math.floor(Date.now() / 1_000);
    for (const [accessToken, expiration] of [
      [await token(), now - 1],
      [await token({ exp: now - 1 }), now + 300],
    ] as const) {
      const encrypted = await new EncryptJWT({ token: accessToken })
        .setProtectedHeader({ alg: 'dir', enc: 'A256GCM' })
        .setIssuer('task-queue-session')
        .setAudience('admin-session')
        .setIssuedAt()
        .setExpirationTime(expiration)
        .encrypt(createHash('sha256').update(secret).digest());
      expect(
        (await request(app).get('/admin/queues').set('Cookie', `admin_session=${encrypted}`))
          .status,
      ).toBe(401);
    }
  });
});
