import { describe, expect, it, vi } from 'vitest';
import { RedisCircuitBreaker } from '../src/resilience/redis-circuit-breaker.js';

describe('RedisCircuitBreaker failures', () => {
  it('surfaces invalid Redis replies and does not execute the operation', async () => {
    const redis = { eval: vi.fn().mockResolvedValue(['invalid', 'generation', 0, 1]) };
    const operation = vi.fn(async () => 'ok');
    const breaker = new RedisCircuitBreaker({
      redis,
      key: 'circuit',
      name: 'billing',
      failureThreshold: 1,
      resetTimeoutMs: 100,
      probeLeaseMs: 100,
    });
    await expect(breaker.execute(operation)).rejects.toThrow('Invalid circuit state');
    expect(operation).not.toHaveBeenCalled();
  });

  it('surfaces a Redis failure recording a successful operation', async () => {
    const redis = {
      eval: vi
        .fn()
        .mockResolvedValueOnce(['closed', 'generation', 0, 1])
        .mockRejectedValueOnce(new Error('Redis unavailable')),
    };
    const breaker = new RedisCircuitBreaker({
      redis,
      key: 'circuit',
      name: 'billing',
      failureThreshold: 1,
      resetTimeoutMs: 100,
      probeLeaseMs: 100,
    });
    await expect(breaker.execute(async () => 'ok')).rejects.toThrow('Redis unavailable');
  });
});
