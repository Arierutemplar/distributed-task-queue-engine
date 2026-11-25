import { z } from 'zod';
import { readFile } from 'node:fs/promises';
import { withTimeout } from '../resilience/timeout.js';

const optional = z.preprocess(
  (value) => (typeof value === 'string' && value.trim() === '' ? undefined : value),
  z.string().optional(),
);
const safePath = (value: string): boolean =>
  value
    .split('/')
    .every((part) => /^[A-Za-z0-9._-]+$/u.test(part) && part !== '.' && part !== '..');

const vaultSchema = z
  .object({
    VAULT_ADDR: z.url(),
    VAULT_AUTH_METHOD: z.enum(['token', 'approle']).default('token'),
    VAULT_TOKEN: optional,
    VAULT_ROLE_ID: optional,
    VAULT_SECRET_ID: optional,
    VAULT_APPROLE_PATH: z.string().default('approle').refine(safePath),
    VAULT_SECRET_PATH: z.string().refine((path) => safePath(path) && path.includes('/data/')),
    VAULT_NAMESPACE: optional,
    VAULT_TIMEOUT_MS: z.coerce.number().int().min(100).max(60_000).default(5_000),
    PRODUCTION_HARDENING_ENABLED: z.enum(['true', 'false']).default('false'),
  })
  .superRefine((settings, context) => {
    const url = new URL(settings.VAULT_ADDR);
    const local = ['localhost', '127.0.0.1', '[::1]', 'openbao', 'vault'].includes(url.hostname);
    if (
      !['http:', 'https:'].includes(url.protocol) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      (url.protocol !== 'https:' && (!local || settings.PRODUCTION_HARDENING_ENABLED === 'true'))
    ) {
      context.addIssue({
        code: 'custom',
        path: ['VAULT_ADDR'],
        message:
          'requires HTTPS without embedded credentials/query/fragment (local development permits HTTP)',
      });
    }
    const required =
      settings.VAULT_AUTH_METHOD === 'token'
        ? (['VAULT_TOKEN'] as const)
        : (['VAULT_ROLE_ID', 'VAULT_SECRET_ID'] as const);
    for (const field of required) {
      if (!settings[field])
        context.addIssue({
          code: 'custom',
          path: [field],
          message: 'is required for the selected Vault authentication method',
        });
    }
  });

const kvResponse = z.object({ data: z.object({ data: z.record(z.string(), z.unknown()) }) });
const loginResponse = z.object({ auth: z.object({ client_token: z.string().min(1) }) });
const sensitiveKeys = new Set([
  'REDIS_URL',
  'REDIS_USERNAME',
  'REDIS_PASSWORD',
  'REDIS_SENTINEL_USERNAME',
  'REDIS_SENTINEL_PASSWORD',
  'API_KEYS',
  'ADMIN_PASSWORD',
  'OIDC_CLIENT_SECRET',
  'SESSION_SECRET',
  'AUDIT_WORM_ACCESS_KEY_ID',
  'AUDIT_WORM_SECRET_ACCESS_KEY',
  'BACKUP_S3_ACCESS_KEY_ID',
  'BACKUP_S3_SECRET_ACCESS_KEY',
  'BACKUP_ENCRYPTION_KEY',
]);

export class SecretsProviderError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SecretsProviderError';
  }
}

/** Loads atomically before env validation/client construction. Vault wins only for sensitive keys. */
export const loadSecrets = async (
  source: NodeJS.ProcessEnv = process.env,
): Promise<{ provider: 'env' | 'vault'; keys: string[] }> => {
  const provider = source.SECRETS_PROVIDER?.trim() ?? 'env';
  if (provider === 'env' || provider === '') {
    if (source.API_KEYS_FILE) {
      try {
        source.API_KEYS = (await readFile(source.API_KEYS_FILE, 'utf8')).trim();
      } catch {
        throw new SecretsProviderError('Unable to read API_KEYS_FILE');
      }
      return { provider: 'env', keys: ['API_KEYS'] };
    }
    return { provider: 'env', keys: [] };
  }
  if (provider !== 'vault') throw new SecretsProviderError('SECRETS_PROVIDER must be env or vault');
  const parsed = vaultSchema.safeParse(source);
  if (!parsed.success) {
    const fields = [...new Set(parsed.error.issues.map((issue) => issue.path.join('.')))];
    throw new SecretsProviderError(
      `Invalid Vault configuration: ${fields.join(', ')} (KV v2 path must be <mount>/data/<secret>)`,
    );
  }
  const settings = parsed.data;
  const address = settings.VAULT_ADDR.replace(/\/$/u, '');
  const headers: Record<string, string> = {};
  if (settings.VAULT_NAMESPACE) headers['X-Vault-Namespace'] = settings.VAULT_NAMESPACE;
  const jsonRequest = async (path: string, init: RequestInit): Promise<unknown> => {
    try {
      return await withTimeout(async (signal) => {
        const response = await fetch(`${address}/v1/${path}`, {
          ...init,
          signal,
          redirect: 'error',
        });
        if (!response.ok)
          throw new SecretsProviderError(`Vault request failed (HTTP ${response.status})`);
        return await response.json();
      }, settings.VAULT_TIMEOUT_MS);
    } catch (error: unknown) {
      if (error instanceof SecretsProviderError) throw error;
      throw new SecretsProviderError(
        'Vault request failed: unavailable, timed out, redirected or invalid JSON',
      );
    }
  };

  let token = settings.VAULT_TOKEN;
  if (settings.VAULT_AUTH_METHOD === 'approle') {
    const login = loginResponse.safeParse(
      await jsonRequest(`auth/${settings.VAULT_APPROLE_PATH}/login`, {
        method: 'POST',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          role_id: settings.VAULT_ROLE_ID,
          secret_id: settings.VAULT_SECRET_ID,
        }),
      }),
    );
    if (!login.success)
      throw new SecretsProviderError('Vault AppRole response did not contain a client token');
    token = login.data.auth.client_token;
  }
  if (!token) throw new SecretsProviderError('Vault authentication did not produce a token');
  const secret = kvResponse.safeParse(
    await jsonRequest(settings.VAULT_SECRET_PATH, {
      headers: { ...headers, 'X-Vault-Token': token },
    }),
  );
  if (!secret.success) throw new SecretsProviderError('Vault response is not a KV v2 secret');
  const loaded: Record<string, string> = {};
  for (const [key, value] of Object.entries(secret.data.data.data)) {
    if (!sensitiveKeys.has(key)) continue;
    if (typeof value !== 'string' || (key !== 'API_KEYS' && !value.trim())) {
      throw new SecretsProviderError(`Vault sensitive key ${key} must be a nonempty string`);
    }
    loaded[key] = value;
  }
  if (Object.keys(loaded).length === 0)
    throw new SecretsProviderError('Vault secret contains no supported sensitive keys');
  Object.assign(source, loaded);
  return { provider: 'vault', keys: Object.keys(loaded) };
};
