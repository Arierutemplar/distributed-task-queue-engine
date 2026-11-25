import type { RedisOptions } from 'ioredis';
import type { AppConfig } from './env.js';

export const resolveRedisConnection = (
  settings: AppConfig,
  sharedOptions: RedisOptions = {},
): { url?: string; options: RedisOptions } => {
  if (settings.REDIS_SENTINEL_HOSTS && settings.REDIS_SENTINEL_MASTER_NAME) {
    return {
      options: {
        ...sharedOptions,
        sentinels: settings.REDIS_SENTINEL_HOSTS,
        name: settings.REDIS_SENTINEL_MASTER_NAME,
        role: 'master',
        username: settings.REDIS_USERNAME,
        password: settings.REDIS_PASSWORD,
        sentinelUsername: settings.REDIS_SENTINEL_USERNAME,
        sentinelPassword: settings.REDIS_SENTINEL_PASSWORD,
        sentinelRetryStrategy: sharedOptions.retryStrategy ?? undefined,
      },
    };
  }
  if (settings.REDIS_URL) return { url: settings.REDIS_URL, options: sharedOptions };
  return {
    options: {
      ...sharedOptions,
      host: settings.REDIS_HOST,
      port: settings.REDIS_PORT,
      username: settings.REDIS_USERNAME,
      password: settings.REDIS_PASSWORD,
    },
  };
};
