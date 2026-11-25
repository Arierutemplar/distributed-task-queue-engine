import pino from 'pino';
import { activeTraceFields } from '../observability/tracing.js';
import { config } from './env.js';

export const logger = pino({
  name: config.SERVICE_NAME,
  level: config.LOG_LEVEL,
  base: { service: config.SERVICE_NAME, role: config.PROCESS_ROLE },
  timestamp: pino.stdTimeFunctions.isoTime,
  // Adds traceId/spanId while a span is active (only when OTEL_ENABLED=true).
  mixin: activeTraceFields,
  redact: {
    paths: [
      'req.headers.authorization',
      'req.headers.cookie',
      'res.headers["set-cookie"]',
      'req.headers["idempotency-key"]',
      'req.headers["x-api-key"]',
      '*.API_KEYS',
      '*.REDIS_URL',
      '*.REDIS_USERNAME',
      '*.password',
      '*.REDIS_PASSWORD',
      '*.ADMIN_PASSWORD',
      '*.OIDC_CLIENT_SECRET',
      '*.SESSION_SECRET',
      '*.AUDIT_WORM_ACCESS_KEY_ID',
      '*.AUDIT_WORM_SECRET_ACCESS_KEY',
      '*.BACKUP_S3_ACCESS_KEY_ID',
      '*.BACKUP_S3_SECRET_ACCESS_KEY',
      '*.BACKUP_ENCRYPTION_KEY',
      '*.VAULT_TOKEN',
      '*.VAULT_ROLE_ID',
      '*.VAULT_SECRET_ID',
      '*.REDIS_SENTINEL_PASSWORD',
      '*.REDIS_SENTINEL_USERNAME',
      'err.command.args',
    ],
    censor: '[REDACTED]',
  },
});
