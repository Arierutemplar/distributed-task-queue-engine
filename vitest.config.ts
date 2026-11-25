import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    // Integration suites need a real Redis: run them with `npm run test:integration`.
    exclude: ['tests/integration/**', 'node_modules/**', 'dist/**'],
    clearMocks: true,
    restoreMocks: true,
    env: {
      NODE_ENV: 'test',
      LOG_LEVEL: 'silent',
      ADMIN_AUTH_MODE: 'basic',
      AUDIT_WORM_ENABLED: 'false',
      SECRETS_PROVIDER: 'env',
      REDIS_SENTINEL_HOSTS: '',
      REDIS_SENTINEL_MASTER_NAME: '',
    },
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      // Process entry point: wiring only, exercised by the integration and Docker smoke tests.
      exclude: ['src/index.ts', 'src/runtime.ts'],
      reporter: ['text-summary', 'text', 'html', 'lcov', 'json-summary'],
      reportsDirectory: 'coverage',
      // Ratchet these up as coverage grows; CI fails below them.
      thresholds: {
        lines: 85,
        statements: 85,
        functions: 83,
        branches: 78,
      },
    },
  },
});
