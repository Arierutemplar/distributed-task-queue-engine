import { redis } from '../config/redis.js';
import { taskQueue } from './task.queue.js';

const acquireScript = `
for _, key in ipairs(KEYS) do
  local kind = redis.call('TYPE', key).ok
  if kind ~= 'none' and kind ~= 'zset' then
    return redis.error_reply('Invalid tenant slot key type')
  end
end
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[1]) then return 0 end
local expires = now + tonumber(ARGV[2])
redis.call('ZADD', KEYS[1], expires, ARGV[3])
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 or ttl < tonumber(ARGV[2]) then redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
redis.call('ZADD', KEYS[2], 'GT', expires, ARGV[4])
return 1
`;

const releaseScript = `
redis.call('ZREM', KEYS[1], ARGV[1])
if redis.call('ZCARD', KEYS[1]) == 0 then redis.call('ZREM', KEYS[2], ARGV[2]) end
`;

const usageScript = `
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local result = {}
for _, tenant in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  local key = ARGV[1] .. tenant
  redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
  local count = redis.call('ZCARD', key)
  if count > 0 then
    table.insert(result, tenant)
    table.insert(result, count)
  else
    redis.call('ZREM', KEYS[1], tenant)
  end
end
return result
`;

const prefix = () => `dtq:tenant-slots:${taskQueue.qualifiedName}:`;
const tenantsKey = () => `dtq:tenant-slot-tenants:${taskQueue.qualifiedName}`;
const slotKey = (tenantId: string) => `${prefix()}${tenantId}`;
const member = (jobId: string, token: string) => JSON.stringify([jobId, token]);

export const acquireTenantSlot = async (
  tenantId: string,
  jobId: string,
  token: string,
  maximum: number,
  leaseMs: number,
): Promise<boolean> => {
  const result = await redis.eval(
    acquireScript,
    2,
    slotKey(tenantId),
    tenantsKey(),
    maximum,
    leaseMs,
    member(jobId, token),
    tenantId,
  );
  if (result !== 0 && result !== 1) throw new Error('Invalid tenant slot acquisition response');
  return result === 1;
};

export const releaseTenantSlot = async (
  tenantId: string,
  jobId: string,
  token: string,
): Promise<void> => {
  await redis.eval(
    releaseScript,
    2,
    slotKey(tenantId),
    tenantsKey(),
    member(jobId, token),
    tenantId,
  );
};

export const readTenantActiveSlots = async (): Promise<Map<string, number>> => {
  const result = await redis.eval(usageScript, 1, tenantsKey(), prefix());
  if (!Array.isArray(result) || result.length % 2 !== 0) {
    throw new Error('Invalid tenant slot usage response');
  }
  const counts = new Map<string, number>();
  for (let index = 0; index < result.length; index += 2) {
    const tenant: unknown = result[index];
    const count: unknown = result[index + 1];
    if (typeof tenant !== 'string' || typeof count !== 'number' || !Number.isSafeInteger(count)) {
      throw new Error('Invalid tenant slot usage entry');
    }
    counts.set(tenant, count);
  }
  return counts;
};
