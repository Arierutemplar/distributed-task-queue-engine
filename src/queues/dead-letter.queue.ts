import { Queue } from 'bullmq';
import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import { redis } from '../config/redis.js';
import type { DeadLetterJobData } from '../types/task.types.js';
import { taskQueue } from './task.queue.js';

export const DEAD_LETTER_QUEUE_NAME = 'tasks-dead-letter';
export const DEAD_LETTER_JOB_NAME = 'dead-letter';

export const deadLetterJobId = (originalJobId: string): string => `dlq-${originalJobId}`;

// Parking queue: no worker consumes it. Entries stay in "waiting" until an operator
// replays or deletes them through the admin API, so they outlive the failed-set retention.
export const deadLetterQueue = new Queue<DeadLetterJobData, void, typeof DEAD_LETTER_JOB_NAME>(
  DEAD_LETTER_QUEUE_NAME,
  {
    connection: redis,
    defaultJobOptions: {
      attempts: 1,
      removeOnComplete: true,
      removeOnFail: false,
    },
  },
);

// Each process exposes only its own counter, preserving Prometheus's per-instance semantics.
const counterKey = deadLetterQueue.toKey(`routed-metrics-${randomUUID()}`);
const counterTtlSeconds = 30 * 24 * 60 * 60;

// This creates the same simple waiting job as BullMQ's addStandardJob script. No worker
// consumes this queue, but its hash, events, pause state and marker must remain BullMQ-compatible.
const routeScript = `
local function checkType(key, expected)
  local actual = redis.call('TYPE', key).ok
  if actual ~= 'none' and actual ~= expected then
    return redis.error_reply('Unexpected Redis key type for ' .. key)
  end
end
local checks = {
  {'zset', KEYS[1]}, {'hash', KEYS[2]}, {'hash', KEYS[3]},
  {'list', KEYS[4]}, {'list', KEYS[5]}, {'hash', KEYS[6]},
  {'stream', KEYS[7]}, {'zset', KEYS[8]}, {'hash', KEYS[9]},
  {'list', KEYS[10]}
}
for _, item in ipairs(checks) do
  local error = checkType(item[2], item[1])
  if error then return error end
end
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) or redis.call('EXISTS', KEYS[2]) == 0 then
  return 0
end
local finishedOn = redis.call('HGET', KEYS[2], 'finishedOn')
local attempts = redis.call('HGET', KEYS[2], 'atm') or '0'
if finishedOn ~= ARGV[8] or attempts ~= ARGV[9] then return 0 end
local failure = finishedOn .. ':' .. attempts
if redis.call('HGET', KEYS[2], 'dtqDlqRouted') == failure or redis.call('EXISTS', KEYS[3]) == 1 then
  return 0
end
local current = redis.call('HGET', KEYS[9], ARGV[4])
if current and (not string.match(current, '^%d+$') or #current > 15 or
  (#current > 1 and string.sub(current, 1, 1) == '0')) then
  return redis.error_reply('Invalid dead-letter counter')
end
local meta = redis.call('HMGET', KEYS[6], 'paused', 'concurrency', 'opts.maxLenEvents')
local concurrency = meta[2] and tonumber(meta[2])
if meta[2] and not concurrency then return redis.error_reply('Invalid queue concurrency') end
local maxEvents = meta[3] and tonumber(meta[3]) or 10000
if not maxEvents or maxEvents < 1 or maxEvents ~= math.floor(maxEvents) or maxEvents > 1000000000 then
  return redis.error_reply('Invalid queue event limit')
end
local paused = meta[1]
local maxed = concurrency and redis.call('LLEN', KEYS[10]) >= concurrency
redis.call('HSET', KEYS[3], 'name', 'dead-letter', 'data', ARGV[2],
  'opts', ARGV[3], 'timestamp', ARGV[5], 'delay', 0, 'priority', 0)
redis.call('XADD', KEYS[7], '*', 'event', 'added', 'jobId', ARGV[6], 'name', 'dead-letter')
redis.call('LPUSH', paused and KEYS[5] or KEYS[4], ARGV[6])
if not paused and not maxed then redis.call('ZADD', KEYS[8], 0, '0') end
redis.call('XADD', KEYS[7], 'MAXLEN', '~', maxEvents, '*', 'event', 'waiting', 'jobId', ARGV[6])
redis.call('HSET', KEYS[2], 'dtqDlqRouted', failure)
redis.call('HINCRBY', KEYS[9], ARGV[4], 1)
redis.call('EXPIRE', KEYS[9], ARGV[7])
return 1
`;

export const addDeadLetter = async (
  data: DeadLetterJobData,
  client: Pick<Redis, 'eval'> = redis,
): Promise<boolean> => {
  const id = deadLetterJobId(data.originalJobId);
  const result = await client.eval(
    routeScript,
    10,
    taskQueue.toKey('failed'),
    taskQueue.toKey(data.originalJobId),
    deadLetterQueue.toKey(id),
    deadLetterQueue.toKey('wait'),
    deadLetterQueue.toKey('paused'),
    deadLetterQueue.toKey('meta'),
    deadLetterQueue.toKey('events'),
    deadLetterQueue.toKey('marker'),
    counterKey,
    deadLetterQueue.toKey('active'),
    data.originalJobId,
    JSON.stringify(data),
    JSON.stringify({ attempts: 1, removeOnComplete: true, removeOnFail: false, jobId: id }),
    data.taskType,
    Date.now(),
    id,
    counterTtlSeconds,
    Date.parse(data.failedAt),
    data.attemptsMade,
  );
  if (result !== 0 && result !== 1) {
    throw new Error('Invalid dead-letter routing reply from Redis');
  }
  return result === 1;
};

export const readDeadLetterCounts = async (): Promise<Record<string, string>> => {
  const counts = await redis.hgetall(counterKey);
  if (Object.keys(counts).length > 0) {
    await redis.expire(counterKey, counterTtlSeconds);
  }
  return counts;
};
