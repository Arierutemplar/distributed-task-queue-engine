import { randomUUID, timingSafeEqual, createHash } from 'node:crypto';
import type { NextFunction, Request, RequestHandler, Response } from 'express';
import type { ApiKeyEntry } from '../config/env.js';
import { httpRequestDuration } from '../observability/metrics.js';
import { PUBLIC_TENANT_ID } from '../types/task.types.js';

const REQUEST_ID_PATTERN = /^[A-Za-z0-9._-]{1,128}$/u;

export const asyncHandler =
  (
    handler: (request: Request, response: Response, next: NextFunction) => Promise<void>,
  ): RequestHandler =>
  (request, response, next) => {
    Promise.resolve(handler(request, response, next)).catch(next);
  };

/** Accepts a well-formed inbound X-Request-Id or generates one, and echoes it back. */
export const requestId: RequestHandler = (request, response, next) => {
  const inbound = request.header('x-request-id');
  const id = inbound && REQUEST_ID_PATTERN.test(inbound) ? inbound : randomUUID();
  response.locals.requestId = id;
  response.setHeader('X-Request-Id', id);
  next();
};

export const getRequestId = (locals: Record<string, unknown>): string =>
  typeof locals.requestId === 'string' ? locals.requestId : randomUUID();

export const httpMetrics: RequestHandler = (request, response, next) => {
  const stopTimer = httpRequestDuration.startTimer();
  response.once('finish', () => {
    const routePath = (request.route as { path?: unknown } | undefined)?.path;
    const route =
      typeof routePath === 'string'
        ? `${request.baseUrl}${routePath}`
        : request.baseUrl || 'unmatched';
    stopTimer({ method: request.method, route, status_code: String(response.statusCode) });
  });
  next();
};

const digest = (value: string): Buffer => createHash('sha256').update(value, 'utf8').digest();

/**
 * Tenant id derived from a plain (legacy) API key. Rotating such a key changes the tenant;
 * to keep it, configure the new key as "<derived-id>:<new-key>".
 */
export const tenantIdForApiKey = (apiKey: string): string =>
  `key-${digest(apiKey).toString('hex').slice(0, 16)}`;

export const getTenantId = (locals: Record<string, unknown>): string => {
  if (typeof locals.tenantId !== 'string') {
    throw new Error('Tenant is not resolved; apiKeyAuth must run before this handler');
  }
  return locals.tenantId;
};

/**
 * Authenticates the task API with an X-API-Key header. A "tenant:key" entry maps the key to
 * a stable tenant (several keys may share one, enabling zero-downtime rotation); a plain key
 * is its own tenant. With no keys configured (development only; production config rejects
 * it) every caller shares PUBLIC_TENANT_ID.
 */
export const apiKeyAuth = (
  apiKeys: readonly ApiKeyEntry[],
  allowAnonymous = true,
): RequestHandler => {
  const tenants = apiKeys.map(({ key, tenantId }) => ({
    digest: digest(key),
    tenantId: tenantId ?? tenantIdForApiKey(key),
  }));

  return (request, response, next) => {
    if (tenants.length === 0 && allowAnonymous) {
      response.locals.tenantId = PUBLIC_TENANT_ID;
      next();
      return;
    }

    const presented = request.header('x-api-key');
    let tenantId: string | undefined;
    if (presented) {
      const presentedDigest = digest(presented);
      // Compare against every key so timing does not reveal which one matched.
      for (const tenant of tenants) {
        if (timingSafeEqual(presentedDigest, tenant.digest)) {
          tenantId ??= tenant.tenantId;
        }
      }
    }

    if (!tenantId) {
      response
        .status(401)
        .json({ error: 'Missing or invalid API key', requestId: getRequestId(response.locals) });
      return;
    }
    response.locals.tenantId = tenantId;
    next();
  };
};

export interface AdminCredentials {
  username: string;
  password?: string;
}

/** Username of the authenticated operator, set by adminAuth; null otherwise. */
export const getAdminActor = (locals: Record<string, unknown>): string | null =>
  typeof locals.adminActor === 'string' ? locals.adminActor : null;

export const getAdminIdentity = (locals: Record<string, unknown>) => ({
  subject: typeof locals.adminSubject === 'string' ? locals.adminSubject : null,
  role: locals.adminRole === 'admin' || locals.adminRole === 'viewer' ? locals.adminRole : null,
});

/**
 * HTTP Basic auth for operator surfaces. Without a configured password the admin
 * surface is disabled (secure by default) instead of being exposed anonymously.
 */
export const adminAuth = (credentials: AdminCredentials): RequestHandler => {
  const expectedUser = digest(credentials.username);
  const expectedPassword = credentials.password ? digest(credentials.password) : undefined;

  return (request, response, next) => {
    if (!expectedPassword) {
      response
        .status(503)
        .json({ error: 'Admin interface is disabled; set ADMIN_PASSWORD to enable it' });
      return;
    }

    const header = request.header('authorization') ?? '';
    const [scheme, encoded] = header.split(' ');
    if (scheme.toLowerCase() === 'basic' && encoded) {
      const decoded = Buffer.from(encoded, 'base64').toString('utf8');
      const separator = decoded.indexOf(':');
      if (separator > 0) {
        const userMatches = timingSafeEqual(digest(decoded.slice(0, separator)), expectedUser);
        const passwordMatches = timingSafeEqual(
          digest(decoded.slice(separator + 1)),
          expectedPassword,
        );
        if (userMatches && passwordMatches) {
          response.locals.adminActor = credentials.username;
          response.locals.adminSubject = credentials.username;
          response.locals.adminRole = 'admin';
          next();
          return;
        }
      }
    }

    response.setHeader('WWW-Authenticate', 'Basic realm="task-queue-admin", charset="UTF-8"');
    response.status(401).json({ error: 'Authentication required' });
  };
};
