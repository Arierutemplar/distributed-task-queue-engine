import { Redis, type RedisOptions } from 'ioredis';
import { config } from './env.js';
import { logger } from './logger.js';
import { resolveRedisConnection } from './redis-options.js';

const retryStrategy = (attempt: number): number => {
  const delayMs = Math.min(500 * 2 ** Math.min(attempt - 1, 6), 30_000);
  logger.warn({ attempt, delayMs }, 'Retrying Redis connection');
  return delayMs;
};

// BullMQ requires maxRetriesPerRequest: null for its blocking commands.
const sharedOptions: RedisOptions = {
  maxRetriesPerRequest: null,
  retryStrategy,
  connectionName: config.SERVICE_NAME,
};

const connection = resolveRedisConnection(config, sharedOptions);
export const redis = connection.url
  ? new Redis(connection.url, connection.options)
  : new Redis(connection.options);

// Coordination commands must fail promptly rather than wait indefinitely in BullMQ's offline queue.
export const coordinationRedis = redis.duplicate({
  connectionName: `${config.SERVICE_NAME}-coordination`,
  maxRetriesPerRequest: 1,
  enableOfflineQueue: false,
  commandTimeout: 1_000,
});
coordinationRedis.on('error', (error: Error) => {
  logger.error({ err: error }, 'Redis coordination connection error');
});

redis.on('connect', () => {
  logger.info('Redis connection established');
});

redis.on('ready', () => {
  logger.info('Redis connection ready');
});

redis.on('reconnecting', (delayMs: number) => {
  logger.warn({ delayMs }, 'Redis reconnecting');
});

redis.on('error', (error: Error) => {
  logger.error({ err: error }, 'Redis connection error');
});

redis.on('close', () => {
  logger.warn('Redis connection closed');
});

redis.on('end', () => {
  logger.info('Redis connection ended');
});

export const pingRedis = async (timeoutMs = 1_000): Promise<void> => {
  let timer: NodeJS.Timeout | undefined;
  try {
    await Promise.race([
      redis.ping(),
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`Redis ping timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
};
