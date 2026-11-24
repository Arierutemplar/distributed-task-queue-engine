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
      'req.headers["idempotency-key"]',
      'req.headers["x-api-key"]',
      '*.API_KEYS',
      '*.password',
      '*.REDIS_PASSWORD',
      '*.ADMIN_PASSWORD',
    ],
    censor: '[REDACTED]',
  },
});
