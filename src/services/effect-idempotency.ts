import { createHash } from 'node:crypto';
import type { Redis } from 'ioredis';
import { invoiceTaskResultSchema, type InvoiceTaskResult } from '../types/task.types.js';

const beginScript = `
local fingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
if fingerprint and fingerprint ~= ARGV[1] then
  return redis.error_reply('Effect idempotency key reused with a different payload')
end
if not fingerprint then
  redis.call('HSET', KEYS[1], 'fingerprint', ARGV[1], 'state', 'in_progress')
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'completed' then
  local result = redis.call('HGET', KEYS[1], 'result')
  if not result then return redis.error_reply('Completed effect has no result') end
  return result
end
if state ~= 'in_progress' then return redis.error_reply('Invalid effect state') end
return false
`;

const completeScript = `
local fingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
if not fingerprint or fingerprint ~= ARGV[1] then
  return redis.error_reply('Effect record missing or payload changed')
end
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'completed' then
  local result = redis.call('HGET', KEYS[1], 'result')
  if not result then return redis.error_reply('Completed effect has no result') end
  return result
end
if state ~= 'in_progress' then return redis.error_reply('Invalid effect state') end
redis.call('HSET', KEYS[1], 'state', 'completed', 'result', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return ARGV[2]
`;

const applyMockScript = `
local fingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
if fingerprint and fingerprint ~= ARGV[1] then
  return redis.error_reply('Payment idempotency key reused with a different payload')
end
if fingerprint then
  local result = redis.call('HGET', KEYS[1], 'result')
  if not result then return redis.error_reply('Payment record has no result') end
  return result
end
redis.call('HSET', KEYS[1], 'fingerprint', ARGV[1], 'result', ARGV[2], 'executions', 1)
redis.call('EXPIRE', KEYS[1], ARGV[3])
return ARGV[2]
`;

export const effectKey = (namespace: string, tenantId: string, effectId: string): string =>
  createHash('sha256')
    .update(JSON.stringify([namespace, tenantId, effectId]))
    .digest('hex');

const parseResult = (value: unknown): InvoiceTaskResult => {
  if (typeof value !== 'string') throw new Error('Invalid effect result from Redis');
  return invoiceTaskResultSchema.parse(JSON.parse(value));
};

export class RedisEffectStore {
  constructor(
    private readonly redis: Pick<Redis, 'eval'>,
    private readonly prefix: string,
    private readonly retentionSeconds: number,
  ) {}

  async begin(key: string, fingerprint: string): Promise<InvoiceTaskResult | undefined> {
    const result = await this.redis.eval(
      beginScript,
      1,
      `${this.prefix}:effect:${key}`,
      fingerprint,
      this.retentionSeconds,
    );
    return result === null ? undefined : parseResult(result);
  }

  async complete(
    key: string,
    fingerprint: string,
    result: InvoiceTaskResult,
  ): Promise<InvoiceTaskResult> {
    return parseResult(
      await this.redis.eval(
        completeScript,
        1,
        `${this.prefix}:effect:${key}`,
        fingerprint,
        JSON.stringify(result),
        this.retentionSeconds,
      ),
    );
  }

  async applyMockPayment(
    key: string,
    fingerprint: string,
    result: InvoiceTaskResult,
  ): Promise<InvoiceTaskResult> {
    return parseResult(
      await this.redis.eval(
        applyMockScript,
        1,
        `${this.prefix}:mock-payment:${key}`,
        fingerprint,
        JSON.stringify(result),
        this.retentionSeconds,
      ),
    );
  }
}
