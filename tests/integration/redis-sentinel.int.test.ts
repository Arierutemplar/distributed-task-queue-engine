import { createServer, type Socket } from 'node:net';
import type { AddressInfo } from 'node:net';
import { Redis } from 'ioredis';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { loadConfig } from '../../src/config/env.js';
import { redis } from '../../src/config/redis.js';
import { resolveRedisConnection } from '../../src/config/redis-options.js';
import { TEST_REDIS_URL } from './constants.js';
import { closeConnections, resetState } from './helpers.js';

beforeEach(resetState);
afterAll(closeConnections);

// Minimal local RESP fake for the Sentinel commands ioredis uses; data commands use real test Redis.
const readCommand = (input: Buffer): { values: string[]; consumed: number } | undefined => {
  const headerEnd = input.indexOf('\r\n');
  if (headerEnd < 0) return undefined;
  const count = Number(input.subarray(1, headerEnd).toString());
  const values: string[] = [];
  let offset = headerEnd + 2;
  for (let index = 0; index < count; index += 1) {
    const lengthEnd = input.indexOf('\r\n', offset);
    if (lengthEnd < 0) return undefined;
    const length = Number(input.subarray(offset + 1, lengthEnd).toString());
    const start = lengthEnd + 2;
    if (input.length < start + length + 2) return undefined;
    values.push(input.subarray(start, start + length).toString());
    offset = start + length + 2;
  }
  return { values, consumed: offset };
};
const bulk = (value: string) => `$${Buffer.byteLength(value)}\r\n${value}\r\n`;

describe('Sentinel resolution without external Sentinel infrastructure', () => {
  it('discovers the master through a local fake Sentinel, reaches test Redis and preserves coordination duplicates', async () => {
    const endpoint = new URL(TEST_REDIS_URL);
    const commands: string[][] = [];
    const sockets = new Set<Socket>();
    const sentinel = createServer((socket) => {
      sockets.add(socket);
      socket.on('close', () => {
        sockets.delete(socket);
      });
      let buffer: Buffer = Buffer.alloc(0);
      socket.on('data', (data) => {
        buffer = Buffer.concat([buffer, data]);
        let parsed = readCommand(buffer);
        while (parsed) {
          buffer = buffer.subarray(parsed.consumed);
          const values = parsed.values;
          commands.push(values);
          const command = values[0].toLowerCase();
          if (command === 'sentinel' && values[1] === 'get-master-addr-by-name') {
            socket.write(`*2\r\n${bulk(endpoint.hostname)}${bulk(endpoint.port || '6379')}`);
          } else if (command === 'sentinel') socket.write('*0\r\n');
          else if (command === 'info')
            socket.write(bulk('redis_version:7.0.0\r\nrole:sentinel\r\nloading:0\r\n'));
          else socket.write('+OK\r\n');
          parsed = readCommand(buffer);
        }
      });
    });
    await new Promise<void>((done) => sentinel.listen(0, '127.0.0.1', done));
    const port = (sentinel.address() as AddressInfo).port;
    const connection = resolveRedisConnection(
      loadConfig({
        REDIS_URL: 'redis://127.0.0.1:1',
        REDIS_SENTINEL_HOSTS: `127.0.0.1:${port}`,
        REDIS_SENTINEL_MASTER_NAME: 'task-queue',
        REDIS_SENTINEL_PASSWORD: 'control-password',
        REDIS_USERNAME: endpoint.username ? decodeURIComponent(endpoint.username) : undefined,
        REDIS_PASSWORD: endpoint.password ? decodeURIComponent(endpoint.password) : undefined,
      }),
      {
        lazyConnect: true,
        db: Number(endpoint.pathname.slice(1)) || 0,
        maxRetriesPerRequest: null,
        retryStrategy: () => 50,
      },
    );
    const client = new Redis(connection.options);
    const coordination = client.duplicate({ maxRetriesPerRequest: 1, enableOfflineQueue: false });
    try {
      await client.connect();
      await coordination.connect();
      expect(await client.ping()).toBe('PONG');
      await client.set('sentinel-resolution-proof', 'resolved');
      expect(await redis.get('sentinel-resolution-proof')).toBe('resolved');
      expect(await coordination.get('sentinel-resolution-proof')).toBe('resolved');
      expect(commands).toContainEqual(['sentinel', 'get-master-addr-by-name', 'task-queue']);
      expect(commands).toContainEqual(['auth', 'control-password']);
      expect(coordination.options).toMatchObject({
        name: 'task-queue',
        maxRetriesPerRequest: 1,
        enableOfflineQueue: false,
      });
    } finally {
      client.disconnect();
      coordination.disconnect();
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((done) =>
        sentinel.close(() => {
          done();
        }),
      );
    }
  });
});
