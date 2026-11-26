import { defineConfig } from 'vitest/config';
import {
  ADMIN_PASSWORD,
  ADMIN_USERNAME,
  API_KEYS,
  SERVICE_NAME,
  TEST_REDIS_URL,
} from './tests/integration/constants.js';

// Integration suite: real Redis, real BullMQ workers. Files run one at a time because they
// share the Redis database and each test flushes it.
export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/integration/**/*.test.ts'],
    globalSetup: ['tests/integration/global-setup.ts'],
    pool: 'forks',
    maxWorkers: 1,
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 30_000,
    teardownTimeout: 10_000,
    clearMocks: true,
    env: {
      NODE_ENV: 'test',
      LOG_LEVEL: process.env.TEST_LOG_LEVEL ?? 'silent',
      SERVICE_NAME,
      REDIS_URL: TEST_REDIS_URL,
      API_KEYS,
      ADMIN_USERNAME,
      ADMIN_PASSWORD,
      ADMIN_AUTH_MODE: 'basic',
      AUDIT_WORM_ENABLED: 'false',
      AUDIT_WORM_DELIVERY: 'direct',
      DLQ_RECONCILE_ENABLED: 'false',
      PRODUCTION_HARDENING_ENABLED: 'false',
      SECRETS_PROVIDER: 'env',
      REDIS_SENTINEL_HOSTS: '',
      REDIS_SENTINEL_MASTER_NAME: '',
      TENANT_MAX_ACTIVE_SLOTS: '0',
      // Short, deterministic retry schedule: 3 attempts, 100ms then 200ms backoff.
      TASK_MAX_ATTEMPTS: '3',
      TASK_BACKOFF_DELAY_MS: '100',
      CIRCUIT_BREAKER_FAILURE_THRESHOLD: '2',
      CIRCUIT_BREAKER_RESET_TIMEOUT_MS: '60000',
      BILLING_FAILURE_RATE: '0',
      RATE_LIMIT_MAX: '0',
      WORKER_CONCURRENCY: '5',
      JOB_TIMEOUT_MS: '5000',
    },
  },
});
