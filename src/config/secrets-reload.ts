import type { RequestHandler } from 'express';
import { apiKeyAuth } from '../middleware/http.middleware.js';
import { apiKeysSchema, type ApiKeyEntry } from './env.js';
import { loadSecrets } from './secrets.js';

export interface SecretsReloader {
  authenticate: RequestHandler;
  reload: () => Promise<void>;
}

export const createSecretsReloader = (
  initialKeys: readonly ApiKeyEntry[],
  source: NodeJS.ProcessEnv,
): SecretsReloader => {
  let secured = initialKeys.length > 0;
  let authenticate = apiKeyAuth(initialKeys);
  let pending = Promise.resolve();

  const reload = (): Promise<void> => {
    const next = pending
      .catch(() => undefined)
      .then(async () => {
        const candidate = { ...source };
        await loadSecrets(candidate);
        const parsed = apiKeysSchema.safeParse(candidate.API_KEYS);
        if (!parsed.success) throw new Error('Reloaded API_KEYS are invalid');
        secured ||= parsed.data.length > 0;
        authenticate = apiKeyAuth(parsed.data, !secured);
      });
    pending = next;
    return next;
  };

  return {
    authenticate: (request, response, next) => {
      authenticate(request, response, next);
    },
    reload,
  };
};
