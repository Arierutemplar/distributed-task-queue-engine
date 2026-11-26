import type { RequestHandler } from 'express';
import { z } from 'zod';
import { apiKeyAuth } from '../middleware/http.middleware.js';
import { apiKeysSchema, envSchema, type ApiKeyEntry } from './env.js';
import { loadSecrets, type SecretsProvider } from './secrets.js';
import { normalizeRedisSecret } from './redis-settings.js';

const hotSchema = z.object({
  ADMIN_PASSWORD: envSchema.shape.ADMIN_PASSWORD,
  OIDC_CLIENT_SECRET: envSchema.shape.OIDC_CLIENT_SECRET,
  SESSION_SECRET: envSchema.shape.SESSION_SECRET,
  AUDIT_WORM_ACCESS_KEY_ID: envSchema.shape.AUDIT_WORM_ACCESS_KEY_ID,
  AUDIT_WORM_SECRET_ACCESS_KEY: envSchema.shape.AUDIT_WORM_SECRET_ACCESS_KEY,
  AUDIT_WORM_ENDPOINT: envSchema.shape.AUDIT_WORM_ENDPOINT,
  AUDIT_WORM_REGION: envSchema.shape.AUDIT_WORM_REGION,
  AUDIT_WORM_BUCKET: envSchema.shape.AUDIT_WORM_BUCKET,
  AUDIT_WORM_FORCE_PATH_STYLE: envSchema.shape.AUDIT_WORM_FORCE_PATH_STYLE,
  AUDIT_WORM_LOCK_MODE: envSchema.shape.AUDIT_WORM_LOCK_MODE,
  AUDIT_WORM_RETENTION_DAYS: envSchema.shape.AUDIT_WORM_RETENTION_DAYS,
  AUDIT_WORM_TIMEOUT_MS: envSchema.shape.AUDIT_WORM_TIMEOUT_MS,
});
type HotSecrets = z.infer<typeof hotSchema>;
type HotKey = keyof HotSecrets;
const hotKeys: HotKey[] = [
  'ADMIN_PASSWORD',
  'OIDC_CLIENT_SECRET',
  'SESSION_SECRET',
  'AUDIT_WORM_ACCESS_KEY_ID',
  'AUDIT_WORM_SECRET_ACCESS_KEY',
  'AUDIT_WORM_ENDPOINT',
  'AUDIT_WORM_REGION',
  'AUDIT_WORM_BUCKET',
  'AUDIT_WORM_FORCE_PATH_STYLE',
  'AUDIT_WORM_LOCK_MODE',
  'AUDIT_WORM_RETENTION_DAYS',
  'AUDIT_WORM_TIMEOUT_MS',
];
const redisKeys = [
  'REDIS_URL',
  'REDIS_USERNAME',
  'REDIS_PASSWORD',
  'REDIS_SENTINEL_USERNAME',
  'REDIS_SENTINEL_PASSWORD',
] as const;

export interface SecretsReloadResult {
  applied: string[];
  restartRequired: boolean;
  restartRequiredKeys: string[];
}

export interface SecretsReloader {
  authenticate: RequestHandler;
  reload: () => Promise<SecretsReloadResult>;
  register: (
    prepare: (
      candidate: HotSecrets,
      changed: HotKey[],
    ) => {
      applied: HotKey[];
      commit: () => void;
    },
  ) => void;
}

export const createSecretsReloader = (
  initialKeys: readonly ApiKeyEntry[],
  source: NodeJS.ProcessEnv,
  provider?: SecretsProvider,
  initialSource: NodeJS.ProcessEnv = source,
): SecretsReloader => {
  let secured = initialKeys.length > 0;
  let authenticate = apiKeyAuth(initialKeys);
  let pending: Promise<void> = Promise.resolve();
  const active = { ...initialSource };
  const redisAtStartup = { ...initialSource };
  const handlers: Parameters<SecretsReloader['register']>[0][] = [];

  const reload = (): Promise<SecretsReloadResult> => {
    const next = pending.then(async () => {
      const candidate = { ...source };
      if (provider) await provider.load(candidate);
      else await loadSecrets(candidate);
      const keys = apiKeysSchema.safeParse(candidate.API_KEYS);
      if (!keys.success) throw new Error('Reloaded API_KEYS are invalid');
      const proposed = { ...active };
      for (const key of hotKeys) {
        if (candidate[key] !== undefined) proposed[key] = candidate[key];
      }
      const hot = hotSchema.safeParse(proposed);
      if (!hot.success)
        throw new Error('Reloaded administrative or archive credentials are invalid');
      const changed = hotKeys.filter((key) => proposed[key] !== active[key]);
      const prepared = handlers.map((handler) => handler(hot.data, changed));
      const appliedHot = prepared.flatMap((item) => item.applied);
      const restartRequiredKeys: string[] = changed.filter((key) => !appliedHot.includes(key));
      for (const key of redisKeys) {
        if (normalizeRedisSecret(candidate[key]) !== normalizeRedisSecret(redisAtStartup[key])) {
          restartRequiredKeys.push(key);
        }
      }
      secured ||= keys.data.length > 0;
      authenticate = apiKeyAuth(keys.data, !secured);
      for (const item of prepared) item.commit();
      for (const key of appliedHot) active[key] = proposed[key];
      return {
        applied: ['API_KEYS', ...appliedHot],
        restartRequired: restartRequiredKeys.length > 0,
        restartRequiredKeys,
      };
    });
    pending = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  };

  return {
    authenticate: (request, response, next) => {
      authenticate(request, response, next);
    },
    reload,
    register: (prepare) => {
      handlers.push(prepare);
    },
  };
};
