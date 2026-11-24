import type { Server } from 'node:http';
import type { Store } from 'express-rate-limit';
import type { Redis } from 'ioredis';
import type { Logger } from 'pino';
import { RedisStore, type RedisReply } from 'rate-limit-redis';
import type { ReadinessCheck } from './routes/health.routes.js';

export const closeServer = (server: Server): Promise<void> =>
  new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
    // Keep-alive sockets would otherwise hold close() open until they time out.
    server.closeIdleConnections();
  });

export interface ShutdownStep {
  name: string;
  close: () => Promise<unknown>;
}

export interface GracefulShutdownOptions {
  logger: Logger;
  timeoutMs: number;
  /** Evaluated when shutdown starts, so steps can reference resources created later. */
  steps: () => ShutdownStep[];
  /** Called if the steps exceed timeoutMs. */
  onTimeout?: () => void;
}

export interface GracefulShutdown {
  /** Resolves to true when every step closed cleanly. Repeated calls share the first run. */
  shutdown: (reason: string) => Promise<boolean>;
  isShuttingDown: () => boolean;
}

/**
 * Ordered shutdown: stop accepting HTTP, let the worker finish active jobs, then close
 * queues and Redis. A failing step is logged and does not prevent the next ones.
 */
export const createGracefulShutdown = (options: GracefulShutdownOptions): GracefulShutdown => {
  let running: Promise<boolean> | undefined;

  const run = async (reason: string): Promise<boolean> => {
    options.logger.info({ reason, timeoutMs: options.timeoutMs }, 'Graceful shutdown started');
    const forceExit = setTimeout(() => {
      options.logger.fatal('Graceful shutdown timed out; forcing exit');
      options.onTimeout?.();
    }, options.timeoutMs);
    forceExit.unref();

    let ok = true;
    for (const step of options.steps()) {
      try {
        await step.close();
      } catch (error: unknown) {
        ok = false;
        options.logger.error({ err: error }, `Failed to close ${step.name}`);
      }
    }

    clearTimeout(forceExit);
    options.logger.info({ ok }, 'Graceful shutdown complete');
    return ok;
  };

  return {
    shutdown: (reason) => {
      if (running) {
        options.logger.warn({ reason }, 'Shutdown already in progress; ignoring signal');
        return running;
      }
      running = run(reason);
      return running;
    },
    isShuttingDown: () => running !== undefined,
  };
};

export interface ReadinessOptions {
  isShuttingDown: () => boolean;
  pingRedis: () => Promise<void>;
  worker?: { isRunning: () => boolean };
}

export const createReadinessChecks = (options: ReadinessOptions): ReadinessCheck[] => {
  const checks: ReadinessCheck[] = [
    {
      name: 'shutdown',
      check: () => {
        if (options.isShuttingDown()) {
          throw new Error('Process is shutting down');
        }
      },
    },
    { name: 'redis', check: options.pingRedis },
  ];
  const { worker } = options;
  if (worker) {
    checks.push({
      name: 'worker',
      check: () => {
        if (!worker.isRunning()) {
          throw new Error('Worker is not running');
        }
      },
    });
  }
  return checks;
};

/** Shared Redis-backed rate-limit counters, so limits hold across API replicas. */
export const redisRateLimitStoreFactory =
  (redis: Redis, keyPrefix: string) =>
  (prefix: string): Store =>
    new RedisStore({
      prefix: `${keyPrefix}:rate-limit:${prefix}`,
      sendCommand: (command: string, ...args: string[]) =>
        redis.call(command, ...args) as Promise<RedisReply>,
    });
