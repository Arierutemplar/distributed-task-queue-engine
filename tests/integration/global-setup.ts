import { Redis } from 'ioredis';
import { TEST_REDIS_URL } from './constants.js';

const describeUrl = (url: string): string => url.replace(/\/\/[^@]*@/u, '//***@');

/**
 * Fails fast with an actionable message when Redis is missing, and refuses to touch a
 * database that already holds data: every test flushes it.
 */
export default async function setup(): Promise<() => Promise<void>> {
  const client = new Redis(TEST_REDIS_URL, {
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  });
  try {
    await client.connect();
    await client.ping();
  } catch (error: unknown) {
    client.disconnect();
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Integration tests need a disposable Redis at ${describeUrl(TEST_REDIS_URL)} (${reason}).\n` +
        'Start one with "npm run redis:test:up" or set TEST_REDIS_URL.',
    );
  }

  const keys = await client.dbsize();
  if (keys > 0 && process.env.TEST_REDIS_ALLOW_FLUSH !== 'true') {
    client.disconnect();
    throw new Error(
      `Refusing to run: ${describeUrl(TEST_REDIS_URL)} already contains ${keys} keys and the suite flushes it. ` +
        'Point TEST_REDIS_URL to a disposable database, or set TEST_REDIS_ALLOW_FLUSH=true if it is one.',
    );
  }

  client.disconnect();

  // A fresh connection: tests deliberately kill Redis clients (CLIENT KILL).
  return async () => {
    const cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 1 });
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        cleanup.flushdb(),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => {
            reject(new Error('Redis integration cleanup timed out'));
          }, 5_000);
        }),
      ]);
    } catch {
      // Teardown must never leave Vitest waiting on a stuck Redis command.
    } finally {
      if (timer) clearTimeout(timer);
      cleanup.disconnect();
    }
  };
}
