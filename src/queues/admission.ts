import { randomUUID } from 'node:crypto';
import { redis } from '../config/redis.js';
import type { TaskJobData, TaskType } from '../types/task.types.js';
import { taskQueue } from './task.queue.js';

export interface AdmissionLimits {
  QUEUE_MAX_WAITING: number;
  TENANT_MAX_INFLIGHT: number;
}

export class BackpressureError extends Error {
  constructor(readonly reason: 'queue' | 'tenant') {
    super(reason === 'queue' ? 'Queue capacity exceeded' : 'Tenant inflight quota exceeded');
    this.name = 'BackpressureError';
  }
}

// Each state transition and this scan run atomically in Redis. Delayed retries count as
// inflight, but not as waiting; terminal jobs and DLQ entries do not consume quota.
const scan = `
local counts = {}
local function count(ids)
  for _, id in ipairs(ids) do
    local raw = redis.call('HGET', ARGV[1] .. id, 'data')
    if not raw then return redis.error_reply('Queue state references a missing job') end
    local ok, data = pcall(cjson.decode, raw)
    if not ok or type(data) ~= 'table' then
      return redis.error_reply('Invalid queued job data')
    end
    local tenant = 'public'
    if type(data.meta) == 'table' and data.meta.tenantId ~= nil then
      if type(data.meta.tenantId) ~= 'string' then
        return redis.error_reply('Invalid queued tenant')
      end
      tenant = data.meta.tenantId
    end
    counts[tenant] = (counts[tenant] or 0) + 1
  end
end
for _, key in ipairs({KEYS[1], KEYS[2], KEYS[3]}) do
  local err = count(redis.call('LRANGE', key, 0, -1))
  if err then return err end
end
for _, key in ipairs({KEYS[4], KEYS[5], KEYS[6]}) do
  local err = count(redis.call('ZRANGE', key, 0, -1))
  if err then return err end
end
`;

const usageScript = `${scan}
local result = {}
for tenant, count in pairs(counts) do
  table.insert(result, tenant)
  table.insert(result, count)
end
return result
`;

const admissionScript = `
local function checkType(key, expected)
  local actual = redis.call('TYPE', key).ok
  if actual ~= 'none' and actual ~= expected then
    return redis.error_reply('Unexpected Redis key type for ' .. key)
  end
end
local checks = {
  {KEYS[1], 'list'}, {KEYS[2], 'list'}, {KEYS[3], 'list'},
  {KEYS[4], 'zset'}, {KEYS[5], 'zset'}, {KEYS[6], 'zset'},
  {KEYS[7], 'hash'}, {KEYS[8], 'stream'}, {KEYS[9], 'zset'},
  {KEYS[10], 'hash'}
}
for _, item in ipairs(checks) do
  local err = checkType(item[1], item[2])
  if err then return err end
end
if redis.call('EXISTS', KEYS[10]) == 1 then return 'existing' end
local waiting = redis.call('LLEN', KEYS[1]) + redis.call('LLEN', KEYS[2]) +
  redis.call('ZCARD', KEYS[5])
if tonumber(ARGV[5]) > 0 and waiting >= tonumber(ARGV[5]) then return 'queue' end
if tonumber(ARGV[6]) > 0 then
  ${scan}
  if (counts[ARGV[7]] or 0) >= tonumber(ARGV[6]) then return 'tenant' end
end
local meta = redis.call('HMGET', KEYS[7], 'paused', 'concurrency', 'opts.maxLenEvents')
local concurrency = meta[2] and tonumber(meta[2])
if meta[2] and not concurrency then return redis.error_reply('Invalid queue concurrency') end
local maxEvents = meta[3] and tonumber(meta[3]) or 10000
if not maxEvents or maxEvents < 1 or maxEvents ~= math.floor(maxEvents) or maxEvents > 1000000000 then
  return redis.error_reply('Invalid queue event limit')
end
local maxed = concurrency and redis.call('LLEN', KEYS[3]) >= concurrency
redis.call('HSET', KEYS[10], 'name', ARGV[3], 'data', ARGV[8], 'opts', ARGV[9],
  'timestamp', ARGV[4], 'delay', 0, 'priority', 0)
redis.call('XADD', KEYS[8], '*', 'event', 'added', 'jobId', ARGV[2], 'name', ARGV[3])
redis.call('LPUSH', meta[1] and KEYS[2] or KEYS[1], ARGV[2])
if not meta[1] and not maxed then redis.call('ZADD', KEYS[9], 0, '0') end
redis.call('XADD', KEYS[8], 'MAXLEN', '~', maxEvents, '*', 'event', 'waiting', 'jobId', ARGV[2])
return 'added'
`;

const stateKeys = () =>
  ['wait', 'paused', 'active', 'delayed', 'prioritized', 'waiting-children'].map((state) =>
    taskQueue.toKey(state),
  );

export const readTenantInflight = async (): Promise<Map<string, number>> => {
  const result = await redis.eval(usageScript, 6, ...stateKeys(), taskQueue.toKey(''));
  if (!Array.isArray(result) || result.length % 2 !== 0) {
    throw new Error('Invalid tenant usage response');
  }
  const counts = new Map<string, number>();
  for (let index = 0; index < result.length; index += 2) {
    const tenant: unknown = result[index];
    const count: unknown = result[index + 1];
    if (typeof tenant !== 'string' || typeof count !== 'number' || !Number.isSafeInteger(count)) {
      throw new Error('Invalid tenant usage entry');
    }
    counts.set(tenant, count);
  }
  return counts;
};

export const addWithAdmission = async (
  type: TaskType,
  data: TaskJobData,
  tenantId: string,
  limits: AdmissionLimits,
  jobId?: string,
): Promise<{ id: string }> => {
  const id = jobId ?? randomUUID();
  const result = await redis.eval(
    admissionScript,
    10,
    ...stateKeys(),
    taskQueue.toKey('meta'),
    taskQueue.toKey('events'),
    taskQueue.toKey('marker'),
    taskQueue.toKey(id),
    taskQueue.toKey(''),
    id,
    type,
    Date.now(),
    limits.QUEUE_MAX_WAITING,
    limits.TENANT_MAX_INFLIGHT,
    tenantId,
    JSON.stringify(data),
    JSON.stringify({ ...taskQueue.defaultJobOptions, jobId: id }),
  );
  if (result === 'queue' || result === 'tenant') throw new BackpressureError(result);
  if (result !== 'added' && result !== 'existing') throw new Error('Invalid queue admission reply');
  return { id };
};
