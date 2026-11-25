import { randomUUID } from 'node:crypto';
import { trace } from '@opentelemetry/api';
import type { Redis } from 'ioredis';
import { CircuitOpenError, type CircuitState } from './circuit-breaker.js';

const acquireScript = `
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
local generation = redis.call('HGET', KEYS[1], 'generation')
if not generation then
  generation = ARGV[1]
  redis.call('HSET', KEYS[1], 'state', state, 'generation', generation, 'failures', 0)
end
if state == 'open' or state == 'half-open' then
  local untilTime = tonumber(redis.call('HGET', KEYS[1], 'until') or '0')
  if untilTime > now then return {state, generation, untilTime - now, 0} end
  state = 'half-open'
  generation = ARGV[1]
  redis.call('HSET', KEYS[1], 'state', state, 'generation', generation, 'until', now + tonumber(ARGV[2]))
  return {state, generation, 0, 1}
end
return {state, generation, 0, 1}
`;

const completeScript = `
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
local generation = redis.call('HGET', KEYS[1], 'generation')
if generation ~= ARGV[1] or state ~= ARGV[2] then return state end
if ARGV[3] == 'success' then
  redis.call('HSET', KEYS[1], 'failures', 0)
  if state == 'half-open' then
    state = 'closed'
    redis.call('HSET', KEYS[1], 'state', state, 'generation', ARGV[4])
  end
else
  local failures = redis.call('HINCRBY', KEYS[1], 'failures', 1)
  if state == 'half-open' or failures >= tonumber(ARGV[5]) then
    local time = redis.call('TIME')
    local now = time[1] * 1000 + math.floor(time[2] / 1000)
    state = 'open'
    redis.call('HSET', KEYS[1], 'state', state, 'generation', ARGV[4], 'until', now + tonumber(ARGV[6]))
  end
end
return state
`;

interface RedisCircuitBreakerOptions {
  redis: Pick<Redis, 'eval'>;
  key: string;
  name: string;
  failureThreshold: number;
  resetTimeoutMs: number;
  probeLeaseMs: number;
  onStateChange?: (state: CircuitState, previous: CircuitState) => void;
}

const parseState = (value: unknown): CircuitState => {
  if (value === 'closed' || value === 'open' || value === 'half-open') return value;
  throw new Error('Invalid circuit state returned by Redis');
};

/** Redis is the authority; coordination errors reject calls rather than bypassing the breaker. */
export class RedisCircuitBreaker {
  private observedState: CircuitState | undefined;

  constructor(private readonly options: RedisCircuitBreakerOptions) {}

  private observe(state: CircuitState): void {
    if (state !== this.observedState) {
      const previous = this.observedState ?? state;
      this.observedState = state;
      this.options.onStateChange?.(state, previous);
    }
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    const reply = await this.options.redis.eval(
      acquireScript,
      1,
      this.options.key,
      randomUUID(),
      this.options.probeLeaseMs,
    );
    if (!Array.isArray(reply) || reply.length !== 4 || typeof reply[1] !== 'string') {
      throw new Error('Invalid circuit acquisition reply from Redis');
    }
    const generation: string = reply[1];
    const state = parseState(reply[0]);
    trace.getActiveSpan()?.setAttribute('circuit_breaker.state', state);
    this.observe(state);
    if (reply[3] !== 0 && reply[3] !== 1) {
      throw new Error('Invalid circuit acquisition decision from Redis');
    }
    if (reply[3] === 0) {
      if (typeof reply[2] !== 'number' || reply[2] < 0) {
        throw new Error('Invalid circuit retry delay from Redis');
      }
      throw new CircuitOpenError(this.options.name, reply[2]);
    }

    const complete = async (outcome: 'success' | 'failure'): Promise<void> => {
      const next = await this.options.redis.eval(
        completeScript,
        1,
        this.options.key,
        generation,
        state,
        outcome,
        randomUUID(),
        this.options.failureThreshold,
        this.options.resetTimeoutMs,
      );
      const nextState = parseState(next);
      trace.getActiveSpan()?.setAttribute('circuit_breaker.state', nextState);
      this.observe(nextState);
    };

    let result: T;
    try {
      result = await operation();
    } catch (error: unknown) {
      try {
        await complete('failure');
      } catch (coordinationError: unknown) {
        throw new AggregateError(
          [error, coordinationError],
          'Billing and circuit coordination failed',
        );
      }
      throw error;
    }
    await complete('success');
    return result;
  }
}
