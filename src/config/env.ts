import './dotenv.js';
import { isIP } from 'node:net';
import { z } from 'zod';

const booleanFromString = z
  .enum(['true', 'false', '1', '0'])
  .transform((value) => value === 'true' || value === '1');

const emptyToUndefined = (value: unknown): unknown =>
  typeof value === 'string' && value.trim() === '' ? undefined : value;

const sentinelHosts = z
  .preprocess(emptyToUndefined, z.string().optional())
  .transform((value, context) => {
    if (!value) return undefined;
    const nodes: { host: string; port: number }[] = [];
    for (const entry of value.split(',')) {
      const match = /^(?:\[([A-Fa-f0-9:]+)\]|([A-Za-z0-9._-]+)):(\d+)$/u.exec(entry.trim());
      const port = Number(match?.[3]);
      const bracketed = entry.trim().startsWith('[');
      const host = match ? (bracketed ? match[1] : match[2]) : undefined;
      if (
        !match ||
        !host ||
        (bracketed && isIP(host) !== 6) ||
        !Number.isInteger(port) ||
        port < 1 ||
        port > 65_535
      ) {
        context.addIssue({
          code: 'custom',
          message: 'must be comma-separated host:port entries (IPv6 uses [address]:port)',
        });
        return z.NEVER;
      }
      nodes.push({ host, port });
    }
    return nodes;
  });

const TENANT_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$/u;
const MIN_API_KEY_LENGTH = 32;

export interface ApiKeyEntry {
  /** Explicit tenant from "tenant:key"; undefined for legacy plain keys (tenant derived from the key). */
  tenantId?: string;
  key: string;
}

/**
 * "tenant:key" binds a key to a stable tenant so keys can be rotated (or several keys can
 * share one tenant) without orphaning its tasks. A plain "key" keeps the legacy behaviour.
 */
export const parseApiKeyEntry = (entry: string): ApiKeyEntry => {
  const separator = entry.indexOf(':');
  if (separator > 0) {
    const tenantId = entry.slice(0, separator);
    if (TENANT_ID_PATTERN.test(tenantId)) {
      return { tenantId, key: entry.slice(separator + 1) };
    }
  }
  return { key: entry };
};

const apiKeysSchema = z.preprocess(
  (value) =>
    typeof value === 'string'
      ? value
          .split(',')
          .map((entry) => entry.trim())
          .filter(Boolean)
      : value,
  z
    .array(z.string())
    .default([])
    .transform((entries) => entries.map(parseApiKeyEntry))
    .superRefine((entries, context) => {
      const seen = new Set<string>();
      entries.forEach(({ key }, index) => {
        if (key.length < MIN_API_KEY_LENGTH) {
          context.addIssue({
            code: 'custom',
            path: [index],
            message: `each API key must be at least ${MIN_API_KEY_LENGTH} characters`,
          });
        }
        if (seen.has(key)) {
          context.addIssue({ code: 'custom', path: [index], message: 'duplicate API key' });
        }
        seen.add(key);
      });
    }),
);

export const envSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
    SERVICE_NAME: z.string().trim().min(1).default('distributed-task-queue'),
    LOG_LEVEL: z
      .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
      .default('info'),
    PORT: z.coerce.number().int().min(1).max(65_535).default(3_000),
    PROCESS_ROLE: z.enum(['all', 'api', 'worker']).default('all'),
    SECRETS_PROVIDER: z.preprocess(emptyToUndefined, z.enum(['env', 'vault']).default('env')),
    TRUST_PROXY: z.coerce.number().int().min(0).max(10).default(0),

    REDIS_URL: z.preprocess(emptyToUndefined, z.url().optional()),
    REDIS_HOST: z.string().trim().min(1).default('127.0.0.1'),
    REDIS_PORT: z.coerce.number().int().min(1).max(65_535).default(6_379),
    REDIS_PASSWORD: z.preprocess(emptyToUndefined, z.string().optional()),
    REDIS_USERNAME: z.preprocess(emptyToUndefined, z.string().optional()),
    REDIS_SENTINEL_HOSTS: sentinelHosts,
    REDIS_SENTINEL_MASTER_NAME: z.preprocess(
      emptyToUndefined,
      z
        .string()
        .trim()
        .regex(/^[A-Za-z0-9._-]+$/u)
        .optional(),
    ),
    REDIS_SENTINEL_USERNAME: z.preprocess(emptyToUndefined, z.string().optional()),
    REDIS_SENTINEL_PASSWORD: z.preprocess(emptyToUndefined, z.string().optional()),

    WORKER_CONCURRENCY: z.coerce.number().int().min(1).max(1_000).default(10),
    JOB_TIMEOUT_MS: z.coerce.number().int().min(1_000).max(600_000).default(30_000),
    SHUTDOWN_TIMEOUT_MS: z.coerce.number().int().min(1_000).max(120_000).default(25_000),
    TASK_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(50).default(5),
    TASK_BACKOFF_DELAY_MS: z.coerce.number().int().min(10).max(600_000).default(2_000),

    BILLING_FAILURE_RATE: z.coerce.number().min(0).max(1).default(0.2),
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: z.coerce.number().int().min(1).max(1_000).default(5),
    CIRCUIT_BREAKER_RESET_TIMEOUT_MS: z.coerce
      .number()
      .int()
      .min(1_000)
      .max(600_000)
      .default(30_000),

    RATE_LIMIT_WINDOW_MS: z.coerce.number().int().min(1_000).default(60_000),
    RATE_LIMIT_MAX: z.coerce.number().int().min(0).default(300),
    RATE_LIMIT_TENANT_MAX: z.coerce.number().int().min(0).default(0),
    CORS_ORIGINS: z.string().trim().default('*'),

    API_KEYS: apiKeysSchema,

    ADMIN_USERNAME: z.string().trim().min(1).default('admin'),
    ADMIN_AUTH_MODE: z.enum(['basic', 'oidc']).default('basic'),
    OIDC_ISSUER_URL: z.preprocess(emptyToUndefined, z.url().optional()),
    OIDC_PUBLIC_ISSUER_URL: z.preprocess(emptyToUndefined, z.url().optional()),
    OIDC_CLIENT_ID: z.string().trim().min(1).default('task-queue-admin'),
    OIDC_CLIENT_SECRET: z.preprocess(emptyToUndefined, z.string().optional()),
    OIDC_REDIRECT_URI: z.preprocess(emptyToUndefined, z.url().optional()),
    SESSION_SECRET: z.preprocess(emptyToUndefined, z.string().min(32).optional()),
    ADMIN_PASSWORD: z.preprocess(emptyToUndefined, z.string().min(12).optional()),
    ADMIN_AUTH_MAX_FAILURES: z.coerce.number().int().min(1).max(1_000).default(10),
    ADMIN_AUTH_WINDOW_MS: z.coerce.number().int().min(1_000).default(900_000),
    AUDIT_LOG_MAX_ENTRIES: z.coerce.number().int().min(0).max(1_000_000).default(10_000),
    AUDIT_WORM_ENABLED: z.preprocess(emptyToUndefined, booleanFromString.default(false)),
    AUDIT_WORM_ENDPOINT: z.preprocess(emptyToUndefined, z.url().optional()),
    AUDIT_WORM_REGION: z.string().trim().min(1).default('us-east-1'),
    AUDIT_WORM_BUCKET: z
      .string()
      .regex(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u)
      .default('audit-worm'),
    AUDIT_WORM_ACCESS_KEY_ID: z.preprocess(emptyToUndefined, z.string().min(1).optional()),
    AUDIT_WORM_SECRET_ACCESS_KEY: z.preprocess(emptyToUndefined, z.string().min(1).optional()),
    AUDIT_WORM_FORCE_PATH_STYLE: z.preprocess(emptyToUndefined, booleanFromString.default(true)),
    AUDIT_WORM_LOCK_MODE: z.enum(['GOVERNANCE', 'COMPLIANCE']).default('GOVERNANCE'),
    AUDIT_WORM_RETENTION_DAYS: z.coerce.number().int().min(1).max(36_500).default(30),
    AUDIT_WORM_TIMEOUT_MS: z.coerce.number().int().min(100).max(60_000).default(5_000),
    METRICS_ENABLED: z.preprocess(emptyToUndefined, booleanFromString.default(true)),

    // Tracing is opt-in; the OTLP exporter reads the standard OTEL_EXPORTER_OTLP_* variables itself.
    OTEL_ENABLED: z.preprocess(emptyToUndefined, booleanFromString.default(false)),
    OTEL_EXPORTER_OTLP_ENDPOINT: z.preprocess(emptyToUndefined, z.url().optional()),
    OTEL_SERVICE_NAME: z.preprocess(emptyToUndefined, z.string().trim().min(1).optional()),
  })
  .superRefine((settings, context) => {
    if (Boolean(settings.REDIS_SENTINEL_HOSTS) !== Boolean(settings.REDIS_SENTINEL_MASTER_NAME)) {
      context.addIssue({
        code: 'custom',
        path: ['REDIS_SENTINEL_HOSTS'],
        message: 'REDIS_SENTINEL_HOSTS and REDIS_SENTINEL_MASTER_NAME must be configured together',
      });
    }
    if (settings.AUDIT_WORM_ENDPOINT) {
      const endpoint = new URL(settings.AUDIT_WORM_ENDPOINT);
      const local = ['localhost', '127.0.0.1', '[::1]', 'minio'].includes(endpoint.hostname);
      if (
        !['https:', 'http:'].includes(endpoint.protocol) ||
        endpoint.username ||
        endpoint.password ||
        endpoint.search ||
        endpoint.hash ||
        (endpoint.protocol !== 'https:' && !local)
      ) {
        context.addIssue({
          code: 'custom',
          path: ['AUDIT_WORM_ENDPOINT'],
          message:
            'must use HTTPS without embedded credentials/query/fragment (local MinIO permits HTTP)',
        });
      }
    }
    if (settings.ADMIN_AUTH_MODE === 'oidc' && settings.PROCESS_ROLE !== 'worker') {
      for (const field of ['OIDC_ISSUER_URL', 'OIDC_REDIRECT_URI', 'SESSION_SECRET'] as const) {
        if (!settings[field]) {
          context.addIssue({ code: 'custom', path: [field], message: 'is required in oidc mode' });
        }
      }
      for (const field of [
        'OIDC_ISSUER_URL',
        'OIDC_PUBLIC_ISSUER_URL',
        'OIDC_REDIRECT_URI',
      ] as const) {
        const value = settings[field];
        if (!value) continue;
        const url = new URL(value);
        const local = ['localhost', '127.0.0.1', '[::1]', 'keycloak'].includes(url.hostname);
        if (
          !['http:', 'https:'].includes(url.protocol) ||
          url.username ||
          url.password ||
          url.search ||
          url.hash ||
          (url.protocol !== 'https:' && !local)
        ) {
          context.addIssue({
            code: 'custom',
            path: [field],
            message:
              'must use HTTPS without credentials/query/fragment (local development permits HTTP)',
          });
        }
      }
    }
    if (
      settings.NODE_ENV === 'production' &&
      settings.PROCESS_ROLE !== 'worker' &&
      settings.API_KEYS.length === 0
    ) {
      context.addIssue({
        code: 'custom',
        path: ['API_KEYS'],
        message: 'is required when NODE_ENV=production and PROCESS_ROLE serves the HTTP API',
      });
    }
  });

export type AppConfig = z.infer<typeof envSchema>;

export class ConfigurationError extends Error {
  constructor(readonly issues: string[]) {
    super(`Invalid configuration:\n  - ${issues.join('\n  - ')}`);
    this.name = 'ConfigurationError';
  }
}

export const loadConfig = (source: NodeJS.ProcessEnv = process.env): AppConfig => {
  const parsed = envSchema.safeParse(source);
  if (!parsed.success) {
    throw new ConfigurationError(
      parsed.error.issues.map(({ path, message }) => `${path.join('.')}: ${message}`),
    );
  }
  return parsed.data;
};

export const config = loadConfig();

export const runsApi = (role: AppConfig['PROCESS_ROLE']): boolean => role !== 'worker';
export const runsWorker = (role: AppConfig['PROCESS_ROLE']): boolean => role !== 'api';
