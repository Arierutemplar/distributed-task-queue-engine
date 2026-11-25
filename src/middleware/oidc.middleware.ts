import { createHash, randomBytes } from 'node:crypto';
import { Router, type CookieOptions, type Request, type RequestHandler } from 'express';
import {
  createRemoteJWKSet,
  EncryptJWT,
  errors,
  jwtDecrypt,
  jwtVerify,
  type JWTPayload,
} from 'jose';
import { z } from 'zod';
import type { AppConfig } from '../config/env.js';
import { adminAuth } from './http.middleware.js';

const roleClaims = z.object({
  realm_access: z.object({ roles: z.array(z.string()) }).optional(),
  resource_access: z.record(z.string(), z.object({ roles: z.array(z.string()) })).optional(),
});

export const resolveAdminRole = (
  payload: JWTPayload,
  clientId: string,
): 'admin' | 'viewer' | undefined => {
  const parsed = roleClaims.safeParse(payload);
  if (!parsed.success) return undefined;
  const roles = [
    ...(parsed.data.realm_access?.roles ?? []),
    ...(parsed.data.resource_access?.[clientId]?.roles ?? []),
  ];
  return roles.includes('admin') ? 'admin' : roles.includes('viewer') ? 'viewer' : undefined;
};

const sessionSchema = z.object({ token: z.string().min(1) });
const transactionSchema = z.object({
  state: z.string().min(1),
  verifier: z.string().min(1),
  nonce: z.string().min(1),
});
const tokenResponseSchema = z.object({
  access_token: z.string().min(1),
  id_token: z.string().min(1),
});

const cookie = (request: Request, name: string): string | undefined => {
  const cookies = (request.header('cookie') ?? '').split(';').map((value) => value.trim());
  const matches = cookies.filter((value) => value.startsWith(`${name}=`));
  if (matches.length > 1) throw new errors.JWTInvalid('Duplicate session cookie');
  return matches[0]?.slice(name.length + 1);
};

const isCredentialError = (error: unknown): boolean =>
  error instanceof errors.JOSEError && !(error instanceof errors.JWKSTimeout);

const random = () => randomBytes(32).toString('base64url');

export const createAdminSecurity = (settings: AppConfig) => {
  const router = Router();
  if (settings.ADMIN_AUTH_MODE === 'basic') {
    return {
      router,
      authenticate: adminAuth({
        username: settings.ADMIN_USERNAME,
        password: settings.ADMIN_PASSWORD,
      }),
    };
  }
  if (!settings.OIDC_ISSUER_URL || !settings.OIDC_REDIRECT_URI || !settings.SESSION_SECRET) {
    throw new Error('OIDC configuration is incomplete');
  }
  const issuer = (settings.OIDC_PUBLIC_ISSUER_URL ?? settings.OIDC_ISSUER_URL).replace(/\/$/u, '');
  const internalIssuer = settings.OIDC_ISSUER_URL.replace(/\/$/u, '');
  const redirectUri = settings.OIDC_REDIRECT_URI;
  const origin = new URL(redirectUri).origin;
  const jwks = createRemoteJWKSet(new URL(`${internalIssuer}/protocol/openid-connect/certs`));
  const key = createHash('sha256').update(settings.SESSION_SECRET).digest();
  const cookieOptions: CookieOptions = {
    httpOnly: true,
    secure: new URL(redirectUri).protocol === 'https:',
    sameSite: 'lax',
    path: '/admin',
  };
  const sessionName = cookieOptions.secure ? '__Secure-admin_session' : 'admin_session';
  const transactionName = cookieOptions.secure ? '__Secure-admin_oidc' : 'admin_oidc';
  router.use((_request, response, next) => {
    response.setHeader('Cache-Control', 'no-store');
    response.setHeader('Referrer-Policy', 'no-referrer');
    next();
  });
  const seal = (payload: JWTPayload, audience: string, expiration: number) =>
    new EncryptJWT(payload)
      .setProtectedHeader({ alg: 'dir', enc: 'A256GCM' })
      .setIssuer('task-queue-session')
      .setAudience(audience)
      .setIssuedAt()
      .setExpirationTime(expiration)
      .encrypt(key);
  const unseal = async (value: string, audience: string) =>
    (
      await jwtDecrypt(value, key, {
        issuer: 'task-queue-session',
        audience,
        keyManagementAlgorithms: ['dir'],
        contentEncryptionAlgorithms: ['A256GCM'],
        requiredClaims: ['exp', 'iat'],
      })
    ).payload;
  const verify = async (token: string) => {
    const { payload } = await jwtVerify(token, jwks, {
      issuer,
      audience: settings.OIDC_CLIENT_ID,
      algorithms: ['RS256'],
      requiredClaims: ['exp', 'sub', 'iat'],
    });
    if (typeof payload.sub !== 'string' || !payload.sub) {
      throw new errors.JWTInvalid('Invalid subject');
    }
    return payload;
  };

  router.get('/login', async (_request, response, next) => {
    try {
      const state = random();
      const verifier = random();
      const nonce = random();
      const transaction = await seal(
        { state, verifier, nonce },
        'oidc-login',
        Math.floor(Date.now() / 1_000) + 600,
      );
      response.cookie(transactionName, transaction, { ...cookieOptions, maxAge: 600_000 });
      const url = new URL(`${issuer}/protocol/openid-connect/auth`);
      url.search = new URLSearchParams({
        client_id: settings.OIDC_CLIENT_ID,
        redirect_uri: redirectUri,
        response_type: 'code',
        scope: 'openid profile',
        state,
        nonce,
        code_challenge: createHash('sha256').update(verifier).digest('base64url'),
        code_challenge_method: 'S256',
      }).toString();
      response.redirect(url.toString());
    } catch (error: unknown) {
      next(error);
    }
  });

  router.get('/callback', async (request, response, next) => {
    response.clearCookie(transactionName, cookieOptions);
    try {
      const encrypted = cookie(request, transactionName);
      if (!encrypted) throw new errors.JWTInvalid('Missing OIDC transaction');
      const parsed = transactionSchema.safeParse(await unseal(encrypted, 'oidc-login'));
      if (
        !parsed.success ||
        typeof request.query.code !== 'string' ||
        typeof request.query.state !== 'string' ||
        request.query.state !== parsed.data.state
      ) {
        throw new errors.JWTInvalid('Invalid OIDC state or code');
      }
      const body = new URLSearchParams({
        grant_type: 'authorization_code',
        client_id: settings.OIDC_CLIENT_ID,
        redirect_uri: redirectUri,
        code: request.query.code,
        code_verifier: parsed.data.verifier,
      });
      if (settings.OIDC_CLIENT_SECRET) body.set('client_secret', settings.OIDC_CLIENT_SECRET);
      const result = await fetch(`${internalIssuer}/protocol/openid-connect/token`, {
        method: 'POST',
        body,
        signal: AbortSignal.timeout(5_000),
      });
      if (result.status === 400 || result.status === 401) {
        throw new errors.JWTInvalid('OIDC code exchange rejected');
      }
      if (!result.ok) throw new Error(`OIDC token endpoint returned ${result.status}`);
      const tokens = tokenResponseSchema.safeParse(await result.json());
      if (!tokens.success) throw new errors.JWTInvalid('Invalid OIDC token response');
      const payload = await verify(tokens.data.access_token);
      const identity = await verify(tokens.data.id_token);
      if (identity.nonce !== parsed.data.nonce || identity.sub !== payload.sub) {
        throw new errors.JWTInvalid('OIDC nonce or subject mismatch');
      }
      response.locals.adminSubject = payload.sub;
      response.locals.adminActor =
        typeof payload.preferred_username === 'string' ? payload.preferred_username : payload.sub;
      response.locals.adminRole = resolveAdminRole(payload, settings.OIDC_CLIENT_ID);
      if (!response.locals.adminRole) {
        response.status(403).json({ error: 'Forbidden' });
        return;
      }
      // No refresh token is stored: a session cannot outlive its validated access token.
      const expiration = payload.exp;
      if (typeof expiration !== 'number') throw new errors.JWTInvalid('Missing expiration');
      const session = await seal({ token: tokens.data.access_token }, 'admin-session', expiration);
      if (Buffer.byteLength(session) > 3_800) {
        throw new Error('OIDC access token exceeds the session cookie size limit');
      }
      response.cookie(sessionName, session, {
        ...cookieOptions,
        maxAge: Math.max(0, expiration * 1_000 - Date.now()),
      });
      response.redirect('/admin/queues');
    } catch (error: unknown) {
      if (isCredentialError(error)) {
        response.status(401).json({ error: 'Invalid OIDC login' });
        return;
      }
      next(error);
    }
  });

  router.get('/logout', (_request, response) => {
    response.clearCookie(sessionName, cookieOptions);
    response.clearCookie(transactionName, cookieOptions);
    const url = new URL(`${issuer}/protocol/openid-connect/logout`);
    url.search = new URLSearchParams({
      client_id: settings.OIDC_CLIENT_ID,
      post_logout_redirect_uri: `${origin}/admin/login`,
    }).toString();
    response.redirect(url.toString());
  });

  const authenticate: RequestHandler = async (request, response, next) => {
    try {
      const authorization = request.header('authorization');
      let token: string;
      let fromSession = false;
      if (authorization !== undefined) {
        const bearer = /^Bearer ([^\s]+)$/iu.exec(authorization);
        if (!bearer?.[1]) throw new errors.JWTInvalid('Invalid Bearer header');
        token = bearer[1];
      } else {
        const encrypted = cookie(request, sessionName);
        if (!encrypted) {
          if (request.method === 'GET' && request.originalUrl.split('?')[0] === '/admin/queues') {
            response.redirect('/admin/login');
            return;
          }
          throw new errors.JWTInvalid('Missing session');
        }
        const parsed = sessionSchema.safeParse(await unseal(encrypted, 'admin-session'));
        if (!parsed.success) throw new errors.JWTInvalid('Invalid session');
        token = parsed.data.token;
        fromSession = true;
      }
      const payload = await verify(token);
      response.locals.adminSubject = payload.sub;
      response.locals.adminActor =
        typeof payload.preferred_username === 'string' ? payload.preferred_username : payload.sub;
      const role = resolveAdminRole(payload, settings.OIDC_CLIENT_ID);
      response.locals.adminRole = role;
      const read = request.method === 'GET' || request.method === 'HEAD';
      if (
        !role ||
        (role === 'viewer' && !read) ||
        (fromSession && !read && request.header('origin') !== origin)
      ) {
        response.status(403).json({ error: 'Forbidden' });
        return;
      }
      next();
    } catch (error: unknown) {
      if (isCredentialError(error)) {
        response.setHeader('WWW-Authenticate', 'Bearer realm="task-queue-admin"');
        response.status(401).json({ error: 'Missing or invalid admin token' });
        return;
      }
      next(error);
    }
  };
  return { router, authenticate };
};
