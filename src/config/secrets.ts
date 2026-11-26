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
    VAULT_TOKEN_RENEW_INTERVAL_MS: z.coerce
      .number()
      .int()
      .min(1_000)
      .max(86_400_000)
      .default(60_000),
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
const renewResponse = z.object({
  auth: z.object({ client_token: z.string().min(1).optional() }),
});
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
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = 'SecretsProviderError';
  }
}

interface RenewalEvent {
  action: 'renew' | 'reauthenticate';
  outcome: 'success' | 'failure';
}

interface VaultSession {
  token?: string;
  start: (operation: () => Promise<RenewalEvent['action']>, interval: number) => void;
}

export interface SecretsProvider {
  load: (source: NodeJS.ProcessEnv) => Promise<{ provider: 'env' | 'vault'; keys: string[] }>;
  close: () => Promise<void>;
}

export const createSecretsProvider = (
  onRenewal: (event: RenewalEvent) => void,
): SecretsProvider => {
  let pending: Promise<void> = Promise.resolve();
  let timer: NodeJS.Timeout | undefined;
  let closed = false;
  let failures = 0;
  const enqueue = <T>(operation: () => Promise<T>): Promise<T> => {
    const result = pending.then(operation, operation);
    pending = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  };
  const session: VaultSession = {
    start(operation, interval) {
      if (timer || closed) return;
      const schedule = (delay: number): void => {
        if (closed) return;
        timer = setTimeout(() => {
          timer = undefined;
          void enqueue(operation)
            .then((action) => {
              failures = 0;
              onRenewal({ action, outcome: 'success' });
              schedule(interval);
            })
            .catch(() => {
              failures += 1;
              onRenewal({ action: 'renew', outcome: 'failure' });
              const delay = Math.min(interval * 2 ** Math.min(failures - 1, 8), 300_000);
              schedule(Math.round(delay * (0.8 + Math.random() * 0.4)));
            });
        }, delay);
        timer.unref();
      };
      schedule(interval);
    },
  };
  return {
    load: (source) =>
      enqueue(() => {
        if (closed) throw new SecretsProviderError('Secrets provider is closed');
        return loadSecrets(source, session);
      }),
    async close() {
      closed = true;
      if (timer) clearTimeout(timer);
      await pending;
    },
  };
};

/** Loads atomically before env validation/client construction. Vault wins only for sensitive keys. */
export const loadSecrets = async (
  source: NodeJS.ProcessEnv = process.env,
  session?: VaultSession,
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
          throw new SecretsProviderError(
            `Vault request failed (HTTP ${response.status})`,
            response.status,
          );
        return await response.json();
      }, settings.VAULT_TIMEOUT_MS);
    } catch (error: unknown) {
      if (error instanceof SecretsProviderError) throw error;
      throw new SecretsProviderError(
        'Vault request failed: unavailable, timed out, redirected or invalid JSON',
      );
    }
  };

  const login = async (): Promise<string> => {
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
    return login.data.auth.client_token;
  };
  let token =
    session?.token ??
    (settings.VAULT_AUTH_METHOD === 'approle' ? await login() : settings.VAULT_TOKEN);
  if (!token) throw new SecretsProviderError('Vault authentication did not produce a token');
  const read = (accessToken: string): Promise<unknown> =>
    jsonRequest(settings.VAULT_SECRET_PATH, {
      headers: { ...headers, 'X-Vault-Token': accessToken },
    });
  let response: unknown;
  try {
    response = await read(token);
  } catch (error: unknown) {
    if (
      !(error instanceof SecretsProviderError) ||
      error.status !== 403 ||
      settings.VAULT_AUTH_METHOD !== 'approle'
    ) {
      throw error;
    }
    token = await login();
    response = await read(token);
  }
  const secret = kvResponse.safeParse(response);
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
  if (session) {
    session.token = token;
    session.start(async () => {
      if (!session.token)
        throw new SecretsProviderError('Vault authentication did not produce a token');
      try {
        const renewed = renewResponse.safeParse(
          await jsonRequest('auth/token/renew-self', {
            method: 'POST',
            headers: { ...headers, 'X-Vault-Token': session.token },
          }),
        );
        if (!renewed.success) throw new SecretsProviderError('Invalid Vault renewal response');
        session.token = renewed.data.auth.client_token ?? session.token;
        return 'renew';
      } catch (error: unknown) {
        if (
          !(error instanceof SecretsProviderError) ||
          (error.status !== 403 && error.status !== 400) ||
          settings.VAULT_AUTH_METHOD !== 'approle'
        ) {
          throw error;
        }
        session.token = await login();
        return 'reauthenticate';
      }
    }, settings.VAULT_TOKEN_RENEW_INTERVAL_MS);
  }
  return { provider: 'vault', keys: Object.keys(loaded) };
};
