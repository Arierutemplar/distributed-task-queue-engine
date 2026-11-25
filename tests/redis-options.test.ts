import { Redis } from 'ioredis';
import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config/env.js';
import { resolveRedisConnection } from '../src/config/redis-options.js';

describe('standalone and Sentinel Redis selection', () => {
  it('preserves URL precedence and standalone connection options', () => {
    const shared = { maxRetriesPerRequest: null, connectionName: 'test' };
    expect(
      resolveRedisConnection(
        loadConfig({ REDIS_URL: 'rediss://user:password@redis.example:6380' }),
        shared,
      ),
    ).toEqual({ url: 'rediss://user:password@redis.example:6380', options: shared });
    expect(
      resolveRedisConnection(
        loadConfig({
          REDIS_HOST: 'db.local',
          REDIS_PORT: '6381',
          REDIS_USERNAME: 'data-user',
          REDIS_PASSWORD: 'data-password',
        }),
        shared,
      ).options,
    ).toMatchObject({
      ...shared,
      host: 'db.local',
      port: 6381,
      username: 'data-user',
      password: 'data-password',
    });
  });

  it('selects Sentinel over REDIS_URL and keeps data and Sentinel authentication separate across duplicates', () => {
    const settings = loadConfig({
      REDIS_URL: 'redis://unused.example:1',
      REDIS_SENTINEL_HOSTS: 'sentinel-a:26379, [::1]:26380',
      REDIS_SENTINEL_MASTER_NAME: 'task-queue',
      REDIS_USERNAME: 'data-user',
      REDIS_PASSWORD: 'data-password',
      REDIS_SENTINEL_USERNAME: 'sentinel-user',
      REDIS_SENTINEL_PASSWORD: 'sentinel-password',
    });
    const retry = () => 100;
    const resolved = resolveRedisConnection(settings, {
      lazyConnect: true,
      maxRetriesPerRequest: null,
      retryStrategy: retry,
    });
    expect(resolved.url).toBeUndefined();
    expect(resolved.options).toMatchObject({
      role: 'master',
      name: 'task-queue',
      sentinels: [
        { host: 'sentinel-a', port: 26379 },
        { host: '::1', port: 26380 },
      ],
      username: 'data-user',
      password: 'data-password',
      sentinelUsername: 'sentinel-user',
      sentinelPassword: 'sentinel-password',
      sentinelRetryStrategy: retry,
      maxRetriesPerRequest: null,
    });
    const client = new Redis(resolved.options);
    const coordination = client.duplicate({ maxRetriesPerRequest: 1, enableOfflineQueue: false });
    expect(coordination.options).toMatchObject({
      sentinels: resolved.options.sentinels,
      name: 'task-queue',
      sentinelPassword: 'sentinel-password',
      password: 'data-password',
      maxRetriesPerRequest: 1,
      enableOfflineQueue: false,
    });
    client.disconnect();
    coordination.disconnect();
  });

  it('rejects incomplete Sentinel settings, malformed nodes and invalid ports instead of silently falling back', () => {
    for (const env of [
      { REDIS_SENTINEL_HOSTS: 'host:26379' },
      { REDIS_SENTINEL_MASTER_NAME: 'task-queue' },
      { REDIS_SENTINEL_HOSTS: 'host:26379', REDIS_SENTINEL_MASTER_NAME: 'bad name' },
      { REDIS_SENTINEL_HOSTS: 'host:0', REDIS_SENTINEL_MASTER_NAME: 'task-queue' },
      { REDIS_SENTINEL_HOSTS: 'host:65536', REDIS_SENTINEL_MASTER_NAME: 'task-queue' },
      { REDIS_SENTINEL_HOSTS: '[abcd]:26379', REDIS_SENTINEL_MASTER_NAME: 'task-queue' },
      { REDIS_SENTINEL_HOSTS: '[:::1]:26379', REDIS_SENTINEL_MASTER_NAME: 'task-queue' },
      {
        REDIS_SENTINEL_HOSTS: 'user:password@host:26379',
        REDIS_SENTINEL_MASTER_NAME: 'task-queue',
      },
    ]) {
      expect(() => loadConfig(env)).toThrow('REDIS_SENTINEL');
    }
  });
});
