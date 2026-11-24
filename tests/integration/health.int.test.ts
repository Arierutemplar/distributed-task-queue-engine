import { Redis } from 'ioredis';
import request from 'supertest';
import type { Express } from 'express';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { redis } from '../../src/config/redis.js';
import { TEST_REDIS_URL } from './constants.js';
import { buildApp, closeConnections, resetState, waitFor } from './helpers.js';

describe('health and readiness against a real Redis', () => {
  let app: Express;
  let control: Redis;

  beforeAll(async () => {
    app = await buildApp();
    control = new Redis(TEST_REDIS_URL);
  });

  beforeEach(resetState);

  afterAll(async () => {
    await control.quit();
    await closeConnections();
  });

  it('reports live and ready, echoing a valid X-Request-Id', async () => {
    const live = await request(app).get('/health/live').set('X-Request-Id', 'it-health-1');
    expect(live.status).toBe(200);
    expect(live.body.status).toBe('ok');
    expect(live.headers['x-request-id']).toBe('it-health-1');

    const ready = await request(app).get('/health/ready');
    expect(ready.status).toBe(200);
    expect(ready.body).toEqual({
      status: 'ready',
      checks: { shutdown: { status: 'up' }, redis: { status: 'up' } },
    });
    expect(ready.headers['x-request-id']).toMatch(/^[0-9a-f-]{36}$/u);
  });

  it('turns not-ready while Redis is unresponsive, stays live, and recovers without restart', async () => {
    // CLIENT PAUSE blocks every client, so the app's PING times out like during an outage.
    await control.call('CLIENT', 'PAUSE', '2500', 'ALL');

    const [ready, live] = await Promise.all([
      request(app).get('/health/ready'),
      request(app).get('/health/live'),
    ]);
    expect(ready.status).toBe(503);
    expect(ready.body.status).toBe('not_ready');
    expect(ready.body.checks.redis).toEqual({
      status: 'down',
      error: 'Redis ping timed out after 1000ms',
    });
    expect(live.status).toBe(200);

    await waitFor(
      async () => (await request(app).get('/health/ready')).status === 200,
      'readiness to recover after the pause',
    );
  });

  it('reconnects after the server drops its connections', async () => {
    await control.call('CLIENT', 'KILL', 'TYPE', 'normal', 'SKIPME', 'yes');

    await waitFor(
      async () => (await request(app).get('/health/ready')).status === 200,
      'readiness after reconnect',
    );
    expect(redis.status).toBe('ready');
  });

  it('serves Prometheus metrics read from Redis', async () => {
    const response = await request(app).get('/metrics');
    expect(response.status).toBe(200);
    expect(response.text).toMatch(/queue_jobs\{queue="tasks",state="waiting"[^}]*\} 0/u);
  });
});
