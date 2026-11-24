import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import request from 'supertest';
import { afterAll, describe, expect, it } from 'vitest';
import { closeServer, createGracefulShutdown } from '../../src/bootstrap.js';
import { logger } from '../../src/config/logger.js';
import { redis } from '../../src/config/redis.js';
import { deadLetterQueue } from '../../src/queues/dead-letter.queue.js';
import { taskQueue } from '../../src/queues/task.queue.js';
import { createTaskWorker } from '../../src/workers/task.worker.js';
import { KEY_A1 } from './constants.js';
import { buildApp, invoicePayload, resetState, waitFor, waitForJobState } from './helpers.js';

describe('graceful shutdown against a real Redis', () => {
  let closed = false;

  afterAll(async () => {
    if (!closed) {
      await taskQueue.close();
      await deadLetterQueue.close();
      await redis.quit();
    }
  });

  it('drains the active job, refuses new connections, and closes Redis in order', async () => {
    await resetState();
    const order: string[] = [];
    let jobStateAtWorkerClose: string | undefined;

    const worker = createTaskWorker({
      billing: {
        processInvoice: async () => {
          await new Promise((resolve) => setTimeout(resolve, 800));
          return {
            taskType: 'PROCESS_INVOICE',
            invoiceId: '0b1c2d3e-4f5a-4b6c-8d7e-9f0a1b2c3d4e',
            status: 'PAID',
            pdfUrl: 'https://billing.example.test/invoices/0b1c2d3e.pdf',
            processedAt: new Date().toISOString(),
          };
        },
      },
    });

    let taskId = '';
    // steps() runs at shutdown time, after `server` below is initialised.
    const lifecycle = createGracefulShutdown({
      logger,
      timeoutMs: 10_000,
      steps: () => [
        {
          name: 'HTTP server',
          close: async () => {
            await closeServer(server);
            order.push('server');
          },
        },
        {
          name: 'task worker',
          close: async () => {
            await worker.close();
            jobStateAtWorkerClose = await (await taskQueue.getJob(taskId))?.getState();
            order.push('worker');
          },
        },
        {
          name: 'task queue',
          close: async () => {
            await taskQueue.close();
            order.push('task queue');
          },
        },
        {
          name: 'dead-letter queue',
          close: async () => {
            await deadLetterQueue.close();
            order.push('dlq');
          },
        },
        {
          name: 'Redis connection',
          close: async () => {
            await redis.quit();
            order.push('redis');
          },
        },
      ],
    });

    const app = await buildApp({ isShuttingDown: lifecycle.isShuttingDown });
    const server = await new Promise<Server>((resolve) => {
      const listening = app.listen(0, '127.0.0.1', () => {
        resolve(listening);
      });
    });
    const baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    // A real HTTP round trip: the request id travels from the header into the job.
    const accepted = await fetch(`${baseUrl}/api/v1/tasks/process`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': KEY_A1,
        'X-Request-Id': 'it-shutdown-1',
      },
      body: JSON.stringify(invoicePayload),
    });
    expect(accepted.status).toBe(202);
    expect(accepted.headers.get('x-request-id')).toBe('it-shutdown-1');
    taskId = ((await accepted.json()) as { taskId: string }).taskId;
    expect((await taskQueue.getJob(taskId))?.data.meta?.requestId).toBe('it-shutdown-1');

    await waitForJobState(taskId, ['active']);
    const shutdown = lifecycle.shutdown('SIGTERM');
    expect(lifecycle.shutdown('SIGINT')).toBe(shutdown);

    // While draining, readiness reports the shutdown so load balancers stop routing traffic.
    const ready = await request(app).get('/health/ready');
    expect(ready.status).toBe(503);
    expect(ready.body.checks.shutdown).toEqual({
      status: 'down',
      error: 'Process is shutting down',
    });

    expect(await shutdown).toBe(true);
    closed = true;
    expect(order).toEqual(['server', 'worker', 'task queue', 'dlq', 'redis']);
    expect(jobStateAtWorkerClose).toBe('completed');
    await waitFor(async () => redis.status === 'end', 'the Redis connection to end');
    await expect(fetch(`${baseUrl}/health/live`)).rejects.toThrow();
  });
});
