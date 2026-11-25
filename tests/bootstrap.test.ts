import { createServer, type Server } from 'node:http';
import { EventEmitter } from 'node:events';
import type { AddressInfo } from 'node:net';
import pino from 'pino';
import { describe, expect, it, vi } from 'vitest';
import {
  closeServer,
  createGracefulShutdown,
  createReadinessChecks,
  redisRateLimitStoreFactory,
  waitForRedisReady,
} from '../src/bootstrap.js';

const logger = pino({ level: 'silent' });

describe('Sentinel coordination startup readiness', () => {
  it('gates initialization until the independently resolving connection is ready', async () => {
    const connection = Object.assign(new EventEmitter(), { status: 'connecting' as const });
    let initialized = false;
    const waiting = waitForRedisReady(connection, 1_000).then(() => {
      initialized = true;
    });
    await Promise.resolve();
    expect(initialized).toBe(false);
    connection.emit('ready');
    await waiting;
    expect(initialized).toBe(true);
    expect(connection.listenerCount('ready')).toBe(0);
    await expect(
      waitForRedisReady(Object.assign(new EventEmitter(), { status: 'ready' as const }), 1_000),
    ).resolves.toBeUndefined();
  });

  it('fails bounded startup and removes listeners when coordination never becomes ready', async () => {
    vi.useFakeTimers();
    try {
      const connection = Object.assign(new EventEmitter(), { status: 'connecting' as const });
      const waiting = expect(waitForRedisReady(connection, 100)).rejects.toThrow('100ms');
      await vi.advanceTimersByTimeAsync(100);
      await waiting;
      expect(connection.listenerCount('ready')).toBe(0);
      expect(connection.listenerCount('error')).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('createGracefulShutdown', () => {
  it('runs the steps in order, keeps going after a failure, and shares one run', async () => {
    const order: string[] = [];
    const lifecycle = createGracefulShutdown({
      logger,
      timeoutMs: 1_000,
      steps: () => [
        { name: 'server', close: async () => order.push('server') },
        {
          name: 'worker',
          close: async () => {
            order.push('worker');
            throw new Error('boom');
          },
        },
        { name: 'redis', close: async () => order.push('redis') },
      ],
    });

    expect(lifecycle.isShuttingDown()).toBe(false);
    const first = lifecycle.shutdown('SIGTERM');
    expect(lifecycle.isShuttingDown()).toBe(true);
    expect(lifecycle.shutdown('SIGINT')).toBe(first);
    await expect(first).resolves.toBe(false);
    expect(order).toEqual(['server', 'worker', 'redis']);
  });

  it('calls onTimeout when a step hangs past the deadline', async () => {
    vi.useFakeTimers();
    try {
      const onTimeout = vi.fn();
      let release = (): void => undefined;
      const lifecycle = createGracefulShutdown({
        logger,
        timeoutMs: 500,
        onTimeout,
        steps: () => [
          { name: 'stuck', close: () => new Promise<void>((resolve) => (release = resolve)) },
        ],
      });

      const done = lifecycle.shutdown('SIGTERM');
      await vi.advanceTimersByTimeAsync(499);
      expect(onTimeout).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(onTimeout).toHaveBeenCalledOnce();

      release();
      await expect(done).resolves.toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('closeServer', () => {
  it('stops listening even with idle keep-alive sockets open', async () => {
    const server: Server = createServer((_request, response) => response.end('ok'));
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as AddressInfo;
    expect((await fetch(`http://127.0.0.1:${port}/`)).status).toBe(200);

    await closeServer(server);
    expect(server.listening).toBe(false);
    await expect(closeServer(server)).rejects.toThrow();
  });
});

describe('createReadinessChecks', () => {
  it('checks shutdown, Redis and, when present, the worker', async () => {
    let shuttingDown = false;
    let workerRunning = true;
    const pingRedis = vi.fn(async () => undefined);
    const checks = createReadinessChecks({
      isShuttingDown: () => shuttingDown,
      pingRedis,
      worker: { isRunning: () => workerRunning },
    });
    expect(checks.map((check) => check.name)).toEqual(['shutdown', 'redis', 'worker']);

    for (const check of checks) {
      await check.check();
    }
    expect(pingRedis).toHaveBeenCalledOnce();

    shuttingDown = true;
    workerRunning = false;
    expect(() => checks[0]?.check()).toThrow('Process is shutting down');
    expect(() => checks[2]?.check()).toThrow('Worker is not running');

    const apiOnly = createReadinessChecks({ isShuttingDown: () => false, pingRedis });
    expect(apiOnly.map((check) => check.name)).toEqual(['shutdown', 'redis']);
  });
});

describe('redisRateLimitStoreFactory', () => {
  it('namespaces counters per service and limiter and sends commands through ioredis', async () => {
    const call = vi.fn(async () => 1);
    const store = redisRateLimitStoreFactory({ call } as never, 'svc')('admin-auth:');

    expect((store as unknown as { prefix: string }).prefix).toBe('svc:rate-limit:admin-auth:');
    await store.resetKey('tenant-a');
    expect(call).toHaveBeenCalledWith('DEL', 'svc:rate-limit:admin-auth:tenant-a');
  });
});
