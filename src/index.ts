import 'dotenv/config';
import type { Server } from 'node:http';
import { z } from 'zod';
import { app } from './app.js';
import { logger, redis } from './config/redis.js';
import { taskQueue } from './queues/task.queue.js';
import { createTaskWorker } from './workers/task.worker.js';

const portSchema = z.coerce.number().int().min(1).max(65_535).default(3_000);
const concurrencySchema = z.coerce.number().int().min(1).max(1_000).default(10);

const closeServer = (server: Server): Promise<void> =>
  new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });

const startServer = async (): Promise<void> => {
  const port = portSchema.parse(process.env.PORT);
  concurrencySchema.parse(process.env.WORKER_CONCURRENCY);
  await redis.ping();

  const worker = createTaskWorker();
  let server: Server | undefined;
  let shuttingDown = false;

  const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    logger.info({ signal }, 'Graceful shutdown started');

    if (server) {
      try {
        await closeServer(server);
      } catch (error: unknown) {
        process.exitCode = 1;
        logger.error({ err: error }, 'Failed to close HTTP server');
      }
    }

    try {
      await worker.close();
    } catch (error: unknown) {
      process.exitCode = 1;
      logger.error({ err: error }, 'Failed to close task worker');
    }

    try {
      await taskQueue.close();
    } catch (error: unknown) {
      process.exitCode = 1;
      logger.error({ err: error }, 'Failed to close task queue');
    }

    try {
      await redis.quit();
    } catch (error: unknown) {
      process.exitCode = 1;
      logger.error({ err: error }, 'Failed to close Redis connection');
    }

    logger.info('Graceful shutdown complete');
  };

  process.once('SIGTERM', () => {
    void shutdown('SIGTERM');
  });
  process.once('SIGINT', () => {
    void shutdown('SIGINT');
  });

  server = await new Promise<Server>((resolve, reject) => {
    const listeningServer = app.listen(port);
    listeningServer.once('error', reject);
    listeningServer.once('listening', () => resolve(listeningServer));
  });

  logger.info({ port }, 'HTTP server listening');
};

void startServer().catch(async (error: unknown) => {
  logger.fatal({ err: error }, 'Failed to start task service');
  try {
    await taskQueue.close();
  } catch (closeError: unknown) {
    logger.error({ err: closeError }, 'Failed to close task queue after startup error');
  }
  try {
    await redis.quit();
  } catch (closeError: unknown) {
    logger.error({ err: closeError }, 'Failed to close Redis after startup error');
  }
  process.exitCode = 1;
});
