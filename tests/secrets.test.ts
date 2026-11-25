import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { loadSecrets } from '../src/config/secrets.js';

let server: Server;
let address: string;
let payload: unknown;
let status = 200;
let hold = false;
let login: unknown;
let calls: { path?: string; token?: string; namespace?: string; body: string }[];
const token = 'test-vault-token';

beforeAll(async () => {
  server = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    calls.push({
      path: req.url,
      token:
        typeof req.headers['x-vault-token'] === 'string' ? req.headers['x-vault-token'] : undefined,
      namespace:
        typeof req.headers['x-vault-namespace'] === 'string'
          ? req.headers['x-vault-namespace']
          : undefined,
      body: Buffer.concat(chunks).toString(),
    });
    if (hold) return;
    res.setHeader('Content-Type', 'application/json');
    if (req.url?.endsWith('/login')) {
      res.end(JSON.stringify(login));
      return;
    }
    res.statusCode = req.headers['x-vault-token'] === token ? status : 403;
    res.end(JSON.stringify(payload));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  address = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
beforeEach(() => {
  calls = [];
  status = 200;
  hold = false;
  login = { auth: { client_token: token } };
  payload = {
    data: {
      data: { ADMIN_PASSWORD: 'from-vault-password', API_KEYS: 'v'.repeat(32), PORT: '6666' },
    },
  };
});
afterAll(async () => {
  const closed = new Promise<void>((resolve) =>
    server.close(() => {
      resolve();
    }),
  );
  server.closeAllConnections();
  await closed;
});
const environment = (overrides: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv => ({
  SECRETS_PROVIDER: 'vault',
  VAULT_ADDR: address,
  VAULT_TOKEN: token,
  VAULT_SECRET_PATH: 'secret/data/task-queue',
  ...overrides,
});

describe('Vault/OpenBao KV v2 secret loading', () => {
  it('does nothing by default, even when unused Vault settings are invalid', async () => {
    const env = { ADMIN_PASSWORD: 'original', VAULT_ADDR: 'not-a-url' };
    expect(await loadSecrets(env)).toEqual({ provider: 'env', keys: [] });
    expect(env.ADMIN_PASSWORD).toBe('original');
    expect(calls).toHaveLength(0);
  });

  it('atomically replaces sensitive keys, preserves other environment values and ignores non-secret keys', async () => {
    const env = environment({
      ADMIN_PASSWORD: 'old-password',
      PORT: '3000',
      REDIS_PASSWORD: 'existing',
    });
    const loaded = await loadSecrets(env);
    expect(loaded).toEqual({ provider: 'vault', keys: ['ADMIN_PASSWORD', 'API_KEYS'] });
    expect(env).toMatchObject({
      ADMIN_PASSWORD: 'from-vault-password',
      API_KEYS: 'v'.repeat(32),
      PORT: '3000',
      REDIS_PASSWORD: 'existing',
    });
    expect(calls[0]).toMatchObject({ path: '/v1/secret/data/task-queue', token });
  });

  it('supports AppRole, custom auth mounts and namespace headers without persisting the login token', async () => {
    const env = environment({
      VAULT_AUTH_METHOD: 'approle',
      VAULT_TOKEN: '',
      VAULT_ROLE_ID: 'role-fixture',
      VAULT_SECRET_ID: 'secret-fixture',
      VAULT_APPROLE_PATH: 'custom-approle',
      VAULT_NAMESPACE: 'enterprise/team',
    });
    await loadSecrets(env);
    expect(calls[0]).toMatchObject({
      path: '/v1/auth/custom-approle/login',
      namespace: 'enterprise/team',
    });
    expect(JSON.parse(calls[0].body)).toEqual({
      role_id: 'role-fixture',
      secret_id: 'secret-fixture',
    });
    expect(calls[1]).toMatchObject({ token, namespace: 'enterprise/team' });
    expect(env.VAULT_TOKEN).toBe('');
  });

  it('fails before reading or changing secrets when configuration is missing or unsafe', async () => {
    for (const overrides of [
      { VAULT_ADDR: '' },
      { VAULT_TOKEN: '' },
      { VAULT_SECRET_PATH: '' },
      { VAULT_ADDR: 'https://user:private@s3.example' },
      { VAULT_ADDR: 'http://external.example' },
      { VAULT_SECRET_PATH: 'secret/data/../escape' },
      { VAULT_AUTH_METHOD: 'approle', VAULT_ROLE_ID: 'role', VAULT_SECRET_ID: '' },
      { SECRETS_PROVIDER: 'invalid' },
    ]) {
      await expect(loadSecrets(environment(overrides))).rejects.toThrow();
    }
    expect(calls).toHaveLength(0);
  });

  it('fails for rejected tokens and server errors without including response bodies or credentials', async () => {
    payload = { errors: [`private token ${token}`] };
    for (const [auth, code] of [
      ['wrong-private-token', 403],
      [token, 503],
    ] as const) {
      status = code;
      const env = environment({ VAULT_TOKEN: auth, ADMIN_PASSWORD: 'unchanged' });
      await expect(loadSecrets(env)).rejects.toThrow(`HTTP ${code}`);
      expect(env.ADMIN_PASSWORD).toBe('unchanged');
    }
  });

  it('rejects wrong KV versions, empty/invalid secrets and malformed AppRole replies without partial updates', async () => {
    for (const value of [
      { data: { ADMIN_PASSWORD: 'kv-v1' } },
      { data: { data: { PORT: '6666' } } },
      { data: { data: { ADMIN_PASSWORD: 'valid', API_KEYS: 42 } } },
      { data: { data: { ADMIN_PASSWORD: '' } } },
    ]) {
      payload = value;
      const env = environment({ ADMIN_PASSWORD: 'original' });
      await expect(loadSecrets(env)).rejects.toThrow();
      expect(env.ADMIN_PASSWORD).toBe('original');
    }
    login = {};
    await expect(
      loadSecrets(
        environment({
          VAULT_AUTH_METHOD: 'approle',
          VAULT_ROLE_ID: 'role',
          VAULT_SECRET_ID: 'secret',
        }),
      ),
    ).rejects.toThrow('client token');
  });

  it('bounds unavailable connections and slow responses', async () => {
    await expect(
      loadSecrets(environment({ VAULT_ADDR: 'http://127.0.0.1:1', VAULT_TIMEOUT_MS: '100' })),
    ).rejects.toThrow('Vault request failed');
    hold = true;
    await expect(loadSecrets(environment({ VAULT_TIMEOUT_MS: '100' }))).rejects.toThrow(
      'timed out',
    );
  });
});
