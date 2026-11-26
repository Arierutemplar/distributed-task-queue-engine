import { isIP } from 'node:net';
import { z } from 'zod';

export const normalizeRedisSecret = (value: string | undefined): string | undefined =>
  value?.trim() ? value : undefined;

const optionalString = z.preprocess(
  (value) => (typeof value === 'string' && value.trim() === '' ? undefined : value),
  z.string().optional(),
);
const hosts = optionalString.transform((value, context) => {
  if (!value) return undefined;
  const nodes: { host: string; port: number }[] = [];
  for (const entry of value.split(',')) {
    const match = /^(?:\[([A-Fa-f0-9:]+)\]|([A-Za-z0-9._-]+)):(\d+)$/u.exec(entry.trim());
    const port = Number(match?.[3]);
    const bracketed = entry.trim().startsWith('[');
    const host = match ? (bracketed ? match[1] : match[2]) : undefined;
    if (
      !match ||
      !host ||
      (bracketed && isIP(host) !== 6) ||
      !Number.isInteger(port) ||
      port < 1 ||
      port > 65_535
    ) {
      context.addIssue({
        code: 'custom',
        message: 'must be comma-separated host:port entries (IPv6 uses [address]:port)',
      });
      return z.NEVER;
    }
    nodes.push({ host, port });
  }
  return nodes;
});
export const redisSettingsSchema = z.object({
  REDIS_URL: optionalString.pipe(z.url().optional()),
  REDIS_HOST: z.string().trim().min(1).default('127.0.0.1'),
  REDIS_PORT: z.coerce.number().int().min(1).max(65_535).default(6_379),
  REDIS_PASSWORD: optionalString,
  REDIS_USERNAME: optionalString,
  REDIS_SENTINEL_HOSTS: hosts,
  REDIS_SENTINEL_MASTER_NAME: optionalString.pipe(
    z
      .string()
      .trim()
      .regex(/^[A-Za-z0-9._-]+$/u)
      .optional(),
  ),
  REDIS_SENTINEL_USERNAME: optionalString,
  REDIS_SENTINEL_PASSWORD: optionalString,
});
export type RedisSettings = z.infer<typeof redisSettingsSchema>;
export const validateRedisPair = (settings: RedisSettings, context: z.RefinementCtx): void => {
  if (Boolean(settings.REDIS_SENTINEL_HOSTS) !== Boolean(settings.REDIS_SENTINEL_MASTER_NAME)) {
    context.addIssue({
      code: 'custom',
      path: ['REDIS_SENTINEL_HOSTS'],
      message: 'REDIS_SENTINEL_HOSTS and REDIS_SENTINEL_MASTER_NAME must be configured together',
    });
  }
};
export const loadRedisSettings = (source: NodeJS.ProcessEnv): RedisSettings =>
  redisSettingsSchema.superRefine(validateRedisPair).parse(source);
