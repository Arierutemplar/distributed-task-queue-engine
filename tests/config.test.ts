import { describe, expect, it } from 'vitest';
import { ConfigurationError, loadConfig, parseApiKeyEntry } from '../src/config/env.js';

describe('loadConfig', () => {
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
