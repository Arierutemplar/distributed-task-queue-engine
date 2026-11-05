import { Redis } from 'ioredis';
import pino from 'pino';

export const logger = pino({
  name: process.env.SERVICE_NAME ?? 'distributed-task-queue',
  level: process.env.LOG_LEVEL ?? 'info',
});

const retryStrategy = (attempt: number): number => {
  const delayMs = Math.min(500 * 2 ** Math.min(attempt - 1, 6), 30_000);
  logger.warn({ attempt, delayMs }, 'Retrying Redis connection');
  return delayMs;
};

const redisUrl = process.env.REDIS_URL;

export const redis = redisUrl
  ? new Redis(redisUrl, {
      maxRetriesPerRequest: null,
      retryStrategy,
    })
  : new Redis({
      host: process.env.REDIS_HOST ?? '127.0.0.1',
      port: Number.parseInt(process.env.REDIS_PORT ?? '6379', 10),
      password: process.env.REDIS_PASSWORD,
      maxRetriesPerRequest: null,
      retryStrategy,
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
