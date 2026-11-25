import { spawn } from 'node:child_process';
import { createServer, type Server } from 'node:http';
import { createServer as createTcpServer, type AddressInfo, type Socket } from 'node:net';
import { resolve } from 'node:path';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { withTimeout } from '../../src/resilience/timeout.js';
import { TEST_REDIS_URL } from './constants.js';
import { closeConnections, resetState, waitFor } from './helpers.js';

const apiKey = 'vault-api-key-0123456789abcdef0123456789';
const password = 'vault-loaded-admin-password';
const vaultToken = 'vault-child-token';
let vaultKeys = apiKey;
let vault: Server;
let address: string;
let redisUrl = TEST_REDIS_URL;

beforeAll(async () => {
  vault = createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json');
    if (req.url === '/v1/auth/approle/login') {
      res.end(JSON.stringify({ auth: { client_token: vaultToken } }));
      return;
    }
    if (req.headers['x-vault-token'] !== vaultToken) {
      res.statusCode = 403;
      res.end(JSON.stringify({ errors: ['never log this private response'] }));
      return;
    }
    res.end(
      JSON.stringify({
        data: {
          data: {
            API_KEYS: vaultKeys,
            ADMIN_PASSWORD: password,
            REDIS_URL: redisUrl,
          },
        },
      }),
    );
  });
  await new Promise<void>((done) => vault.listen(0, '127.0.0.1', done));
  address = `http://127.0.0.1:${(vault.address() as AddressInfo).port}`;
});
beforeEach(async () => {
  redisUrl = TEST_REDIS_URL;
  vaultKeys = apiKey;
  await resetState();
});
afterAll(async () => {
  const closed = new Promise<void>((done) =>
    vault.close(() => {
      done();
    }),
  );
  vault.closeAllConnections();
  await closed;
  await closeConnections();
});

const freePort = async (): Promise<number> => {
  const server = createServer();
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const port = (server.address() as AddressInfo).port;
  await new Promise<void>((done) =>
    server.close(() => {
      done();
    }),
  );
  return port;
};
const start = (env: NodeJS.ProcessEnv) => {
  const child = spawn(process.execPath, ['--import', 'tsx', resolve('src', 'index.ts')], {
    env: {
      ...process.env,
      VITEST: 'true',
      NODE_ENV: 'production',
      LOG_LEVEL: 'info',
      SECRETS_PROVIDER: 'vault',
      VAULT_ADDR: address,
      VAULT_TOKEN: vaultToken,
      VAULT_AUTH_METHOD: 'token',
      VAULT_ROLE_ID: '',
      VAULT_SECRET_ID: '',
      VAULT_NAMESPACE: '',
      VAULT_TIMEOUT_MS: '5000',
      VAULT_SECRET_PATH: 'secret/data/task-queue',
      API_KEYS: '',
      ADMIN_USERNAME: 'admin',
      ADMIN_PASSWORD: '',
      REDIS_URL: 'redis://127.0.0.1:1',
      REDIS_PASSWORD: '',
      REDIS_SENTINEL_HOSTS: '',
      REDIS_SENTINEL_MASTER_NAME: '',
      OTEL_ENABLED: 'false',
      AUDIT_WORM_ENABLED: 'false',
      ...env,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.setEncoding('utf8').on('data', (chunk: string) => {
    output += chunk;
  });
  child.stderr.setEncoding('utf8').on('data', (chunk: string) => {
    output += chunk;
  });
  const exited = new Promise<number | null>((done, reject) => {
    child.once('error', reject);
    child.once('exit', (code) => {
      done(code);
    });
  });
  return { child, exited, output: () => output };
};

describe('preload before runtime configuration for API and worker entry points', () => {
  it('reloads Vault keys through the admin endpoint and periodically, without reopening access', async () => {
    const port = await freePort();
    const process = start({
      PROCESS_ROLE: 'api',
      PORT: String(port),
      SECRETS_REFRESH_INTERVAL_MS: '100',
    });
    const base = `http://127.0.0.1:${port}`;
    const nextKey = 'vault-next-key-0123456789abcdef01234567';
    const check = (key?: string) =>
      fetch(`${base}/api/v1/tasks/missing`, { headers: key ? { 'X-API-Key': key } : {} });
    const authorization = `Basic ${Buffer.from(`admin:${password}`).toString('base64')}`;
    try {
      await waitFor(async () => {
        if (process.child.exitCode !== null) throw new Error(process.output());
        try {
          return (await fetch(`${base}/health/ready`)).ok;
        } catch (error: unknown) {
          if (error instanceof TypeError) return false;
          throw error;
        }
      }, 'child process readiness');
      vaultKeys = `acme:${apiKey},acme:${nextKey}`;
      expect(
        (
          await fetch(`${base}/admin/api/secrets/reload`, {
            method: 'POST',
            headers: { Authorization: authorization },
          })
        ).status,
      ).toBe(200);
      expect((await check(apiKey)).status).toBe(404);
      expect((await check(nextKey)).status).toBe(404);
      vaultKeys = `acme:${nextKey}`;
      await waitFor(async () => (await check(apiKey)).status === 401, 'periodic key revocation');
      expect((await check(nextKey)).status).toBe(404);
      vaultKeys = 'invalid-short-key';
      expect(
        (
          await fetch(`${base}/admin/api/secrets/reload`, {
            method: 'POST',
            headers: { Authorization: authorization },
          })
        ).status,
      ).toBe(500);
      expect((await check(nextKey)).status).toBe(404);
      vaultKeys = '';
      expect(
        (
          await fetch(`${base}/admin/api/secrets/reload`, {
            method: 'POST',
            headers: { Authorization: authorization },
          })
        ).status,
      ).toBe(200);
      expect((await check()).status).toBe(401);
      expect((await check(nextKey)).status).toBe(401);
      const audit = (await (
        await fetch(`${base}/admin/api/audit`, {
          headers: { Authorization: authorization },
        })
      ).json()) as { items: { action: string; outcome: string }[] };
      expect(audit.items).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ action: 'admin.secrets_reload', outcome: 'success' }),
        ]),
      );
    } finally {
      process.child.kill();
      await process.exited;
    }
  });
  it('does not leak Vault-loaded credentials in Redis authentication error command arguments', async () => {
    const privatePassword = 'disposable-auth-secret-0123456789';
    const sockets = new Set<Socket>();
    const peerErrors: NodeJS.ErrnoException[] = [];
    const endpoint = createTcpServer((socket) => {
      sockets.add(socket);
      socket.on('error', (error: NodeJS.ErrnoException) => {
        peerErrors.push(error);
      });
      socket.on('close', () => {
        sockets.delete(socket);
      });
      socket.on('data', () => {
        socket.write('-WRONGPASS invalid username-password pair\r\n');
      });
    });
    await new Promise<void>((done) => endpoint.listen(0, '127.0.0.1', done));
    redisUrl = `redis://default:${privatePassword}@127.0.0.1:${(endpoint.address() as AddressInfo).port}`;
    const process = start({ PROCESS_ROLE: 'api', PORT: String(await freePort()) });
    try {
      await waitFor(
        async () => process.output().includes('WRONGPASS'),
        'Redis authentication error',
      );
      expect(process.output()).not.toContain(privatePassword);
      expect(process.output()).not.toContain('HTTP server listening');
    } finally {
      process.child.kill();
      await process.exited;
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((done) =>
        endpoint.close(() => {
          done();
        }),
      );
    }
    // The intentionally rejected/killed client can reset its TCP connection on Windows.
    for (const error of peerErrors) expect(error.code, error.message).toBe('ECONNRESET');
  });

  it.each(['api', 'worker'])(
    'starts %s using KV v2 secrets despite initially invalid/empty environment credentials',
    async (role) => {
      const port = await freePort();
      const process = start({
        PROCESS_ROLE: role,
        PORT: String(port),
        ...(role === 'worker'
          ? {
              VAULT_AUTH_METHOD: 'approle',
              VAULT_ROLE_ID: 'role',
              VAULT_SECRET_ID: 'private-secret-id',
              VAULT_TOKEN: '',
            }
          : { VAULT_AUTH_METHOD: 'token' }),
      });
      try {
        const base = `http://127.0.0.1:${port}`;
        await waitFor(async () => {
          if (process.child.exitCode !== null) throw new Error(process.output());
          try {
            return (await fetch(`${base}/health/ready`)).ok;
          } catch (error: unknown) {
            if (error instanceof TypeError) return false;
            throw error;
          }
        }, 'child process readiness');
        if (role === 'api') {
          expect((await fetch(`${base}/api/v1/tasks/missing`)).status).toBe(401);
          expect(
            (await fetch(`${base}/api/v1/tasks/missing`, { headers: { 'X-API-Key': apiKey } }))
              .status,
          ).toBe(404);
          expect(
            (
              await fetch(`${base}/admin/api/audit`, {
                headers: {
                  Authorization: `Basic ${Buffer.from(`admin:${password}`).toString('base64')}`,
                },
              })
            ).status,
          ).toBe(200);
        } else {
          const ready = (await (await fetch(`${base}/health/ready`)).json()) as {
            checks: { worker: { status: string } };
          };
          expect(ready.checks.worker.status).toBe('up');
        }
        expect(process.output()).toContain('Runtime secrets loaded');
        for (const value of [apiKey, password, vaultToken, 'private-secret-id']) {
          expect(process.output()).not.toContain(value);
        }
      } finally {
        process.child.kill();
        await process.exited;
      }
    },
  );

  it('exits nonzero before opening HTTP or Redis when Vault rejects the token', async () => {
    const process = start({
      PROCESS_ROLE: 'api',
      PORT: String(await freePort()),
      VAULT_TOKEN: 'private-invalid-token',
      VAULT_AUTH_METHOD: 'token',
    });
    try {
      expect(await withTimeout(() => process.exited, 5_000)).toBe(1);
      expect(process.output()).toContain('HTTP 403');
      expect(process.output()).not.toContain('Redis connection');
      expect(process.output()).not.toContain('private-invalid-token');
      expect(process.output()).not.toContain('never log this private response');
    } finally {
      if (process.child.exitCode === null) process.child.kill();
      await process.exited;
    }
  });
});
