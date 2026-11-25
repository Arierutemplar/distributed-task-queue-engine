import request from 'supertest';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createApp } from '../../src/app.js';
import { redisRateLimitStoreFactory } from '../../src/bootstrap.js';
import { loadConfig } from '../../src/config/env.js';
import { coordinationRedis, redis } from '../../src/config/redis.js';
import { CircuitOpenError } from '../../src/resilience/circuit-breaker.js';
import { RedisCircuitBreaker } from '../../src/resilience/redis-circuit-breaker.js';
import { ADMIN_PASSWORD, API_KEYS, KEY_A1, KEY_A2, KEY_B, SERVICE_NAME } from './constants.js';
import { closeConnections, resetState, waitFor } from './helpers.js';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const failure = async (): Promise<never> => {
  throw new Error('billing failed');
};

describe('distributed coordination', () => {
  beforeAll(async () => {
    if (coordinationRedis.status !== 'ready') {
      await new Promise<void>((resolve) => coordinationRedis.once('ready', resolve));
    }
  });
  beforeEach(resetState);
  afterAll(closeConnections);

  it('shares tenant quotas between API instances and rotated keys, but isolates tenants', async () => {
    const config = loadConfig({
      NODE_ENV: 'test',
      API_KEYS,
      RATE_LIMIT_MAX: '0',
      RATE_LIMIT_TENANT_MAX: '2',
      ADMIN_PASSWORD,
    });
    const createRateLimitStore = redisRateLimitStoreFactory(coordinationRedis, SERVICE_NAME);
    const [first, second] = await Promise.all([
      createApp({ config, createRateLimitStore }),
      createApp({ config, createRateLimitStore }),
    ]);
    expect(
      (await request(first).get('/api/v1/tasks/missing').set('X-API-Key', KEY_A1)).status,
    ).toBe(404);
    expect(
      (await request(second).get('/api/v1/tasks/missing').set('X-API-Key', KEY_A2)).status,
    ).toBe(404);
    expect(
      (await request(first).get('/api/v1/tasks/missing').set('X-API-Key', KEY_A1)).status,
    ).toBe(429);
    expect(
      (await request(second).get('/api/v1/tasks/missing').set('X-API-Key', KEY_B)).status,
    ).toBe(404);
  });

  it('shares IP and failed admin login quotas between API instances', async () => {
    const config = loadConfig({
      NODE_ENV: 'test',
      API_KEYS,
      RATE_LIMIT_MAX: '1',
      ADMIN_AUTH_MAX_FAILURES: '1',
      ADMIN_PASSWORD,
    });
    const createRateLimitStore = redisRateLimitStoreFactory(coordinationRedis, SERVICE_NAME);
    const [first, second] = await Promise.all([
      createApp({ config, createRateLimitStore }),
      createApp({ config, createRateLimitStore }),
    ]);
    expect((await request(first).get('/api/v1/tasks/missing')).status).toBe(401);
    expect((await request(second).get('/api/v1/tasks/missing')).status).toBe(429);
    expect((await request(first).get('/admin/queues')).status).toBe(401);
    expect((await request(second).get('/admin/queues')).status).toBe(429);
  });

  it('opens across breakers, permits one probe, and fences stale completions', async () => {
    const options = {
      redis: coordinationRedis,
      key: `${SERVICE_NAME}:test-circuit`,
      name: 'test',
      failureThreshold: 2,
      resetTimeoutMs: 120,
      probeLeaseMs: 250,
    };
    const first = new RedisCircuitBreaker(options);
    const second = new RedisCircuitBreaker(options);
    let finishOld: (value: string) => void = () => undefined;
    let started: () => void = () => undefined;
    const startedPromise = new Promise<void>((resolve) => (started = resolve));
    const old = first.execute(() => {
      started();
      return new Promise<string>((resolve) => (finishOld = resolve));
    });
    await startedPromise;
    await expect(first.execute(failure)).rejects.toThrow('billing failed');
    await expect(second.execute(failure)).rejects.toThrow('billing failed');
    finishOld('late');
    await expect(old).resolves.toBe('late');
    const blocked = vi.fn(async () => 'must not run');
    await expect(second.execute(blocked)).rejects.toBeInstanceOf(CircuitOpenError);
    expect(blocked).not.toHaveBeenCalled();

    await delay(150);
    let finishProbe: (value: string) => void = () => undefined;
    let probeStarted: () => void = () => undefined;
    const probeStartedPromise = new Promise<void>((resolve) => (probeStarted = resolve));
    const probe = first.execute(() => {
      probeStarted();
      return new Promise<string>((resolve) => (finishProbe = resolve));
    });
    await probeStartedPromise;
    await expect(second.execute(blocked)).rejects.toBeInstanceOf(CircuitOpenError);
    finishProbe('ok');
    await expect(probe).resolves.toBe('ok');
    await expect(second.execute(async () => 'closed')).resolves.toBe('closed');
  });

  it('recovers abandoned probes and ignores their late results', async () => {
    const options = {
      redis: coordinationRedis,
      key: `${SERVICE_NAME}:lease-circuit`,
      name: 'test',
      failureThreshold: 1,
      resetTimeoutMs: 50,
      probeLeaseMs: 80,
    };
    const first = new RedisCircuitBreaker(options);
    const second = new RedisCircuitBreaker(options);
    await expect(first.execute(failure)).rejects.toThrow('billing failed');
    await delay(70);
    let finish: (value: string) => void = () => undefined;
    let started: () => void = () => undefined;
    const startedPromise = new Promise<void>((resolve) => (started = resolve));
    const oldProbe = first.execute(() => {
      started();
      return new Promise<string>((resolve) => (finish = resolve));
    });
    await startedPromise;
    await delay(100);
    await expect(second.execute(failure)).rejects.toThrow('billing failed');
    finish('late');
    await oldProbe;
    await expect(first.execute(async () => 'unexpected')).rejects.toBeInstanceOf(CircuitOpenError);
  });

  it('counts concurrently completed failures across independent Redis connections', async () => {
    const clients = Array.from({ length: 8 }, () =>
      coordinationRedis.duplicate({ lazyConnect: true }),
    );
    try {
      await Promise.all(clients.map((client) => client.connect()));
      const key = `${SERVICE_NAME}:concurrent-failures`;
      const breakers = clients.map(
        (client) =>
          new RedisCircuitBreaker({
            redis: client,
            key,
            name: 'test',
            failureThreshold: 32,
            resetTimeoutMs: 60_000,
            probeLeaseMs: 60_000,
          }),
      );
      let release: () => void = () => undefined;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const progress = { started: 0 };
      const attempts = Array.from({ length: 32 }, (_, index) =>
        breakers[index % breakers.length].execute(async () => {
          progress.started += 1;
          await gate;
          throw new Error('billing failed');
        }),
      );
      const results = Promise.allSettled(attempts);
      try {
        await waitFor(async () => progress.started === 32, 'all closed-state calls to start');
      } finally {
        release();
      }
      expect((await results).every((result) => result.status === 'rejected')).toBe(true);
      expect(await coordinationRedis.hmget(key, 'state', 'failures')).toEqual(['open', '32']);
    } finally {
      await Promise.all(clients.map((client) => client.quit()));
    }
  });
  it.each(['success', 'failure'] as const)(
    'allows exactly one concurrent half-open probe across Redis clients on %s',
    async (outcome) => {
      const clients = Array.from({ length: 8 }, () =>
        coordinationRedis.duplicate({ lazyConnect: true }),
      );
      try {
        await Promise.all(clients.map((client) => client.connect()));
        const key = `${SERVICE_NAME}:concurrent-probe`;
        const breakers = clients.map(
          (client) =>
            new RedisCircuitBreaker({
              redis: client,
              key,
              name: 'test',
              failureThreshold: 1,
              resetTimeoutMs: 60_000,
              probeLeaseMs: 60_000,
            }),
        );
        await expect(breakers[0]?.execute(failure)).rejects.toThrow('billing failed');
        await coordinationRedis.hset(key, 'until', '0');

        let release: () => void = () => undefined;
        const gate = new Promise<void>((resolve) => (release = resolve));
        let started = 0;
        let rejected = 0;
        const attempts = Array.from({ length: 32 }, (_, index) => {
          const breaker = breakers[index % breakers.length];
          return breaker
            .execute(async () => {
              started += 1;
              await gate;
              if (outcome === 'failure') throw new Error('probe failed');
              return 'ok';
            })
            .catch((error: unknown) => {
              if (error instanceof CircuitOpenError) rejected += 1;
              throw error;
            });
        });
        const results = Promise.allSettled(attempts);
        try {
          await waitFor(async () => started === 1 && rejected === 31, 'one reserved probe');
          expect(await coordinationRedis.hget(key, 'state')).toBe('half-open');
        } finally {
          release();
        }
        const settled = await results;
        expect(started).toBe(1);
        expect(rejected).toBe(31);
        expect(settled.filter((result) => result.status === 'fulfilled')).toHaveLength(
          outcome === 'success' ? 1 : 0,
        );
        expect(await coordinationRedis.hget(key, 'state')).toBe(
          outcome === 'success' ? 'closed' : 'open',
        );
      } finally {
        await Promise.all(clients.map((client) => client.quit()));
      }
    },
  );
  it('fails closed without executing billing if Redis coordination is unavailable', async () => {
    const client = redis.duplicate({ lazyConnect: true, enableOfflineQueue: false });
    const breaker = new RedisCircuitBreaker({
      redis: client,
      key: `${SERVICE_NAME}:unavailable`,
      name: 'test',
      failureThreshold: 1,
      resetTimeoutMs: 100,
      probeLeaseMs: 100,
    });
    const operation = vi.fn(async () => 'ok');
    try {
      await expect(breaker.execute(operation)).rejects.toThrow();
      expect(operation).not.toHaveBeenCalled();
    } finally {
      client.disconnect();
    }
  });
});
