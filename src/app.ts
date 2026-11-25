import cors from 'cors';
import express, {
  type ErrorRequestHandler,
  type Express,
  type Request,
  type Response,
} from 'express';
import { rateLimit, type Store } from 'express-rate-limit';
import helmet from 'helmet';
import type { LevelWithSilent } from 'pino';
import { pinoHttp } from 'pino-http';
import { z } from 'zod';
import { config, runsApi, type AppConfig } from './config/env.js';
import { logger } from './config/logger.js';
import {
  apiKeyAuth,
  getAdminActor,
  getAdminIdentity,
  getRequestId,
  getTenantId,
  httpMetrics,
  requestId,
} from './middleware/http.middleware.js';
import { createAdminSecurity } from './middleware/oidc.middleware.js';
import { createAuditLog, type AuditAction, type AuditLog } from './observability/audit.js';
import { metricsRegistry } from './observability/metrics.js';
import { traceRequests } from './observability/tracing.js';
import { IdempotencyConflictError } from './services/task.service.js';
import { createHealthRouter, type ReadinessCheck } from './routes/health.routes.js';
import { BackpressureError } from './queues/admission.js';
import { backpressureRejections } from './observability/metrics.js';
import type { SecretsReloader } from './config/secrets-reload.js';

export interface AppOptions {
  config?: AppConfig;
  readinessChecks?: ReadinessCheck[];
  /** Builds one store per limiter; express-rate-limit rejects a store shared between limiters. */
  createRateLimitStore?: (prefix: string) => Store;
  /** Audit trail for admin actions; defaults to structured log lines only (no persistence). */
  auditLog?: AuditLog;
  secretsReloader?: SecretsReloader;
}

const parseCorsOrigins = (value: string): cors.CorsOptions['origin'] =>
  value === '*'
    ? '*'
    : value
        .split(',')
        .map((origin) => origin.trim())
        .filter(Boolean);

const isBodyParserError = (error: unknown): error is { type: string; status: number } =>
  typeof error === 'object' && error !== null && 'type' in error && 'status' in error;

const auditLimitSchema = z.coerce.number().int().min(1).max(1_000).default(100);

// Responses on the admin surface that are security relevant even without a handler-level record.
const adminAccessAction = (request: Request, statusCode: number): AuditAction | undefined => {
  if (statusCode === 401) return 'admin.auth_failed';
  if (statusCode === 403) return 'admin.access_denied';
  if (statusCode === 429) return 'admin.auth_locked';
  if (statusCode === 503) return 'admin.disabled';
  // Bull Board mutations (retry, clean, promote, remove...) are operator actions too.
  if (request.originalUrl.startsWith('/admin/queues/api/') && request.method !== 'GET') {
    return 'admin.bull_board_action';
  }
  return undefined;
};

export const createApp = async (options: AppOptions = {}): Promise<Express> => {
  const settings = options.config ?? config;
  const audit = options.auditLog ?? createAuditLog({ logger });
  const app = express();

  app.disable('x-powered-by');
  app.set('trust proxy', settings.TRUST_PROXY);

  app.use(requestId);
  app.use(traceRequests);
  app.use(
    pinoHttp<Request, Response>({
      logger,
      serializers: {
        req: (request: { url?: string }) => ({
          ...request,
          url:
            request.url && /^\/admin\/callback(?:\/|\?|$)/iu.test(request.url)
              ? '/admin/callback'
              : request.url,
        }),
      },
      genReqId: (_request, response) => getRequestId(response.locals),
      customSuccessObject: (_request, response, fields: Record<string, unknown>) => ({
        ...fields,
        requestId: getRequestId(response.locals),
      }),
      customErrorObject: (_request, response, _error, fields: Record<string, unknown>) => ({
        ...fields,
        requestId: getRequestId(response.locals),
      }),
      autoLogging: {
        ignore: (request) => request.url.startsWith('/health') || request.url === '/metrics',
      },
      customLogLevel: (request, response, error): LevelWithSilent => {
        if (error || response.statusCode >= 500) return 'error';
        if (response.statusCode >= 400) return 'warn';
        // Bull Board polls its API and loads assets every few seconds; keep that out of info.
        if (request.method === 'GET' && request.originalUrl.startsWith('/admin/queues')) {
          return 'debug';
        }
        return 'info';
      },
    }),
  );
  app.use(httpMetrics);
  app.use((_request, response, next) => {
    response.locals.admissionLimits = settings;
    next();
  });

  app.use('/health', createHealthRouter(options.readinessChecks ?? []));

  if (settings.METRICS_ENABLED) {
    app.get('/metrics', async (_request, response, next) => {
      try {
        response.setHeader('Content-Type', metricsRegistry.contentType);
        response.send(await metricsRegistry.metrics());
      } catch (error: unknown) {
        next(error);
      }
    });
  }

  if (runsApi(settings.PROCESS_ROLE)) {
    // Lazy imports keep a worker-only process from loading the API and Bull Board graph.
    const [{ taskRouter }, { boardRouter }, { createDeadLetterRouter }] = await Promise.all([
      import('./routes/task.routes.js'),
      import('./routes/board.routes.js'),
      import('./routes/dead-letter.routes.js'),
    ]);

    const apiLimiter = rateLimit({
      windowMs: settings.RATE_LIMIT_WINDOW_MS,
      limit: settings.RATE_LIMIT_MAX,
      standardHeaders: 'draft-8',
      legacyHeaders: false,
      passOnStoreError: true,
      skip: () => settings.RATE_LIMIT_MAX === 0,
      ...(options.createRateLimitStore ? { store: options.createRateLimitStore('tasks:') } : {}),
      handler: (_request, response) => {
        response.status(429).json({ error: 'Too many requests' });
      },
    });

    // Optional per-tenant quota, keyed by the authenticated tenant rather than the client IP.
    const tenantLimiter = rateLimit({
      windowMs: settings.RATE_LIMIT_WINDOW_MS,
      limit: settings.RATE_LIMIT_TENANT_MAX,
      standardHeaders: 'draft-8',
      legacyHeaders: false,
      passOnStoreError: true,
      skip: () => settings.RATE_LIMIT_TENANT_MAX === 0,
      keyGenerator: (_request, response) => getTenantId(response.locals),
      ...(options.createRateLimitStore ? { store: options.createRateLimitStore('tenant:') } : {}),
      handler: (_request, response) => {
        response.status(429).json({ error: 'Tenant request quota exceeded' });
      },
    });

    // Limits by client IP before authentication so invalid keys cannot be tried without bound.
    app.use(
      '/api/v1/tasks',
      helmet(),
      cors({
        origin: parseCorsOrigins(settings.CORS_ORIGINS),
        exposedHeaders: ['X-Request-Id', 'Location', 'Idempotent-Replayed', 'Retry-After'],
      }),
      apiLimiter,
      options.secretsReloader?.authenticate ?? apiKeyAuth(settings.API_KEYS),
      tenantLimiter,
      express.json({ limit: '1mb' }),
      taskRouter,
    );

    // Only rejected credentials count, so Bull Board's polling never locks out an operator.
    const adminAuthLimiter = rateLimit({
      windowMs: settings.ADMIN_AUTH_WINDOW_MS,
      limit: settings.ADMIN_AUTH_MAX_FAILURES,
      standardHeaders: 'draft-8',
      legacyHeaders: false,
      passOnStoreError: true,
      skipSuccessfulRequests: true,
      requestWasSuccessful: (_request, response) => response.statusCode !== 401,
      ...(options.createRateLimitStore
        ? { store: options.createRateLimitStore('admin-auth:') }
        : {}),
      handler: (_request, response) => {
        response.status(429).json({ error: 'Too many failed admin login attempts' });
      },
    });

    const security = createAdminSecurity(settings);
    app.use('/admin', (request, response, next) => {
      response.once('finish', () => {
        const action = adminAccessAction(request, response.statusCode);
        if (!action) {
          return;
        }
        void audit.record({
          action,
          outcome: response.statusCode < 400 ? 'success' : 'denied',
          actor: getAdminActor(response.locals),
          ...getAdminIdentity(response.locals),
          requestId: getRequestId(response.locals),
          method: request.method,
          path: request.originalUrl.split('?')[0] ?? request.originalUrl,
          ip: request.ip,
          statusCode: response.statusCode,
        });
      });
      next();
    });
    app.use('/admin', adminAuthLimiter);
    app.use('/admin', security.router, security.authenticate);
    app.post('/admin/api/secrets/reload', async (request, response, next) => {
      if (response.locals.adminRole !== 'admin') {
        response.status(403).json({ error: 'Forbidden' });
        return;
      }
      const event = {
        action: 'admin.secrets_reload' as const,
        actor: getAdminActor(response.locals),
        ...getAdminIdentity(response.locals),
        requestId: getRequestId(response.locals),
        method: request.method,
        path: '/admin/api/secrets/reload',
        ip: request.ip,
      };
      if (!options.secretsReloader) {
        await audit.record({ ...event, outcome: 'error', statusCode: 503 });
        response.status(503).json({ error: 'Secrets reload is unavailable' });
        return;
      }
      try {
        await options.secretsReloader.reload();
        await audit.record({ ...event, outcome: 'success', statusCode: 200 });
        response.json({ status: 'reloaded' });
      } catch (error: unknown) {
        await audit.record({ ...event, outcome: 'error', statusCode: 500 });
        next(error);
      }
    });
    app.get('/admin/api/audit', async (request, response, next) => {
      const limit = auditLimitSchema.safeParse(request.query.limit);
      if (!limit.success) {
        response.status(400).json({ error: 'Invalid limit' });
        return;
      }
      try {
        const items = await audit.list(limit.data);
        await audit.record({
          action: 'admin.audit_read',
          outcome: 'success',
          actor: getAdminActor(response.locals),
          ...getAdminIdentity(response.locals),
          requestId: getRequestId(response.locals),
          method: request.method,
          path: '/admin/api/audit',
          ip: request.ip,
          statusCode: 200,
        });
        response.json({ items });
      } catch (error: unknown) {
        next(error);
      }
    });
    app.use(
      '/admin/api/dead-letter',
      express.json({ limit: '16kb' }),
      createDeadLetterRouter(audit),
    );
    app.use('/admin/queues', boardRouter);
  }

  app.use((_request, response) => {
    response
      .status(404)
      .json({ error: 'Route not found', requestId: getRequestId(response.locals) });
  });

  const errorHandler: ErrorRequestHandler = (
    error: unknown,
    _request: Request,
    response: Response,
    _next,
  ) => {
    const requestIdValue = getRequestId(response.locals);

    if (isBodyParserError(error)) {
      if (error.type === 'entity.parse.failed') {
        response
          .status(400)
          .json({ error: 'Invalid JSON request body', requestId: requestIdValue });
        return;
      }
      if (error.type === 'entity.too.large') {
        response
          .status(413)
          .json({ error: 'Request body exceeds 1 MB', requestId: requestIdValue });
        return;
      }
      if (error.status >= 400 && error.status < 500) {
        response
          .status(error.status)
          .json({ error: 'Invalid request body', requestId: requestIdValue });
        return;
      }
    }

    if (error instanceof IdempotencyConflictError) {
      response.status(409).json({ error: error.message, requestId: requestIdValue });
      return;
    }

    if (error instanceof BackpressureError) {
      backpressureRejections.inc({ reason: error.reason });
      response.setHeader('Retry-After', '1');
      response.status(429).json({ error: error.message, requestId: requestIdValue });
      return;
    }

    logger.error({ err: error, requestId: requestIdValue }, 'Unhandled request error');
    response.status(500).json({ error: 'Internal server error', requestId: requestIdValue });
  };

  app.use(errorHandler);

  return app;
};
