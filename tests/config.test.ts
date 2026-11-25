import { describe, expect, it } from 'vitest';
import { ConfigurationError, loadConfig, parseApiKeyEntry } from '../src/config/env.js';

describe('loadConfig', () => {
  it('gates operational hardening without changing the default Basic/standalone configuration', () => {
    expect(loadConfig({}).PRODUCTION_HARDENING_ENABLED).toBe(false);
    const production: NodeJS.ProcessEnv = {
      NODE_ENV: 'production',
      API_KEYS: 'k'.repeat(32),
      PRODUCTION_HARDENING_ENABLED: 'true',
      SHUTDOWN_TIMEOUT_MS: '45000',
      DLQ_RECONCILE_ENABLED: 'true',
      AUDIT_WORM_ENABLED: 'true',
      AUDIT_WORM_DELIVERY: 'outbox',
      AUDIT_WORM_ACCESS_KEY_ID: 'writer',
      AUDIT_WORM_SECRET_ACCESS_KEY: 'secret',
      AUDIT_WORM_LOCK_MODE: 'COMPLIANCE',
      REDIS_PASSWORD: 'private-password',
      ADMIN_AUTH_MODE: 'oidc',
      SESSION_SECRET: 's'.repeat(32),
      TRUST_PROXY: '1',
      CORS_ORIGINS: 'https://queue.example.com',
      OIDC_ISSUER_URL: 'https://id.example.com/realm',
      OIDC_PUBLIC_ISSUER_URL: 'https://id.example.com/realm',
      OIDC_REDIRECT_URI: 'https://queue.example.com/admin/callback',
    };
    expect(loadConfig(production).PROCESS_ROLE).toBe('all');
    for (const override of [
      { NODE_ENV: 'test' },
      { DLQ_RECONCILE_ENABLED: 'false' },
      { SHUTDOWN_TIMEOUT_MS: '25000' },
      { AUDIT_WORM_DELIVERY: 'direct' },
      { AUDIT_WORM_LOCK_MODE: 'GOVERNANCE' },
      { AUDIT_WORM_ENDPOINT: 'http://minio:9000' },
      { REDIS_PASSWORD: '' },
      { REDIS_URL: 'redis://localhost:6379' },
      { ADMIN_AUTH_MODE: 'basic' },
      { TRUST_PROXY: '0' },
      { CORS_ORIGINS: '*' },
      { OIDC_PUBLIC_ISSUER_URL: '' },
    ])
      expect(() => loadConfig({ ...production, ...override })).toThrow(ConfigurationError);
    expect(
      loadConfig({
        ...production,
        PROCESS_ROLE: 'worker',
        ADMIN_AUTH_MODE: 'basic',
        TRUST_PROXY: '0',
      }).PROCESS_ROLE,
    ).toBe('worker');
  });
  it('applies documented defaults', () => {
    expect(loadConfig({})).toMatchObject({
      PORT: 3_000,
      PROCESS_ROLE: 'all',
      WORKER_CONCURRENCY: 10,
      JOB_TIMEOUT_MS: 30_000,
      BILLING_FAILURE_RATE: 0.2,
      METRICS_ENABLED: true,
    });
    expect(loadConfig({}).ADMIN_PASSWORD).toBeUndefined();
  });

  it.each([
    [{ WORKER_CONCURRENCY: '0' }, 'WORKER_CONCURRENCY'],
    [{ WORKER_CONCURRENCY: '1001' }, 'WORKER_CONCURRENCY'],
    [{ QUEUE_MAX_WAITING: '-1' }, 'QUEUE_MAX_WAITING'],
    [{ TENANT_MAX_INFLIGHT: '-1' }, 'TENANT_MAX_INFLIGHT'],
    [{ EFFECT_RETENTION_SECONDS: '3599' }, 'EFFECT_RETENTION_SECONDS'],
    [{ SECRETS_REFRESH_INTERVAL_MS: '-1' }, 'SECRETS_REFRESH_INTERVAL_MS'],
    [{ PROCESS_ROLE: 'scheduler' }, 'PROCESS_ROLE'],
    [{ ADMIN_PASSWORD: 'short' }, 'ADMIN_PASSWORD'],
    [{ BILLING_FAILURE_RATE: '1.5' }, 'BILLING_FAILURE_RATE'],
    [{ API_KEYS: 'too-short' }, 'API_KEYS'],
    [{ NODE_ENV: 'production' }, 'API_KEYS'],
    [{ NODE_ENV: 'production', PROCESS_ROLE: 'api' }, 'API_KEYS'],
  ])('rejects %o', (env, field) => {
    expect(() => loadConfig(env)).toThrow(ConfigurationError);
    expect(() => loadConfig(env)).toThrow(new RegExp(field, 'u'));
  });

  it('parses API_KEYS and only requires them in production for roles that serve the API', () => {
    const key = 'k'.repeat(32);
    expect(loadConfig({ API_KEYS: ` ${key} , ${'j'.repeat(40)} ,` }).API_KEYS).toEqual([
      { key },
      { key: 'j'.repeat(40) },
    ]);
    expect(loadConfig({}).API_KEYS).toEqual([]);
    expect(loadConfig({ NODE_ENV: 'production', PROCESS_ROLE: 'worker' }).API_KEYS).toEqual([]);
    expect(loadConfig({ NODE_ENV: 'production', API_KEYS: key }).API_KEYS).toEqual([{ key }]);
  });

  it('maps "tenant:key" entries to a stable tenant and keeps plain keys backward compatible', () => {
    const current = 'c'.repeat(32);
    const next = 'n'.repeat(32);
    expect(
      loadConfig({ API_KEYS: `acme:${current},acme:${next},${'p'.repeat(32)}` }).API_KEYS,
    ).toEqual([
      { tenantId: 'acme', key: current },
      { tenantId: 'acme', key: next },
      { key: 'p'.repeat(32) },
    ]);
    // A prefix that is not a valid tenant id keeps the whole entry as the key.
    expect(parseApiKeyEntry(`not a tenant:${current}`)).toEqual({ key: `not a tenant:${current}` });
    expect(() => loadConfig({ API_KEYS: `a:${current},b:${current}` })).toThrow(
      /duplicate API key/u,
    );
    expect(() => loadConfig({ API_KEYS: 'acme:short' })).toThrow(/at least 32 characters/u);
  });

  it('keeps retry defaults and allows tuning them', () => {
    expect(loadConfig({})).toMatchObject({
      TASK_MAX_ATTEMPTS: 5,
      TASK_BACKOFF_DELAY_MS: 2_000,
      OTEL_ENABLED: false,
    });
    expect(loadConfig({ TASK_MAX_ATTEMPTS: '3', TASK_BACKOFF_DELAY_MS: '50' })).toMatchObject({
      TASK_MAX_ATTEMPTS: 3,
      TASK_BACKOFF_DELAY_MS: 50,
    });
  });
});
