import dotenv from 'dotenv';
import { z } from 'zod';

// Tests must be hermetic: a developer's .env (for example a cloud REDIS_URL) must never leak in.
if (process.env.VITEST === undefined) {
  dotenv.config({ quiet: true });
}

const booleanFromString = z
  .enum(['true', 'false', '1', '0'])
  .transform((value) => value === 'true' || value === '1');

const emptyToUndefined = (value: unknown): unknown =>
  typeof value === 'string' && value.trim() === '' ? undefined : value;

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
    TRUST_PROXY: z.coerce.number().int().min(0).max(10).default(0),

    REDIS_URL: z.preprocess(emptyToUndefined, z.url().optional()),
    REDIS_HOST: z.string().trim().min(1).default('127.0.0.1'),
    REDIS_PORT: z.coerce.number().int().min(1).max(65_535).default(6_379),
    REDIS_PASSWORD: z.preprocess(emptyToUndefined, z.string().optional()),

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
    ADMIN_PASSWORD: z.preprocess(emptyToUndefined, z.string().min(12).optional()),
    ADMIN_AUTH_MAX_FAILURES: z.coerce.number().int().min(1).max(1_000).default(10),
    ADMIN_AUTH_WINDOW_MS: z.coerce.number().int().min(1_000).default(900_000),
    AUDIT_LOG_MAX_ENTRIES: z.coerce.number().int().min(0).max(1_000_000).default(10_000),
    METRICS_ENABLED: z.preprocess(emptyToUndefined, booleanFromString.default(true)),

    // Tracing is opt-in; the OTLP exporter reads the standard OTEL_EXPORTER_OTLP_* variables itself.
    OTEL_ENABLED: z.preprocess(emptyToUndefined, booleanFromString.default(false)),
    OTEL_EXPORTER_OTLP_ENDPOINT: z.preprocess(emptyToUndefined, z.url().optional()),
  })
  .superRefine((settings, context) => {
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
