import { Redis, type RedisOptions } from 'ioredis';
import { config } from './env.js';
import { logger } from './logger.js';

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

export const redis = config.REDIS_URL
  ? new Redis(config.REDIS_URL, sharedOptions)
  : new Redis({
      ...sharedOptions,
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      password: config.REDIS_PASSWORD,
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
