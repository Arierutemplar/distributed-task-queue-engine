import { describe, expect, it } from 'vitest';
import { CircuitBreaker, CircuitOpenError } from '../src/resilience/circuit-breaker.js';

const fail = async (): Promise<never> => {
  throw new Error('boom');
};

const createBreaker = () => {
  let now = 0;
  const breaker = new CircuitBreaker({
    name: 'test',
    failureThreshold: 2,
    resetTimeoutMs: 1_000,
    now: () => now,
  });
  return { breaker, advance: (ms: number) => (now += ms) };
};

describe('CircuitBreaker', () => {
  it('opens after consecutive failures and resets the count on success', async () => {
    const { breaker } = createBreaker();

    await expect(breaker.execute(fail)).rejects.toThrow('boom');
    await expect(breaker.execute(async () => 'ok')).resolves.toBe('ok');
    await expect(breaker.execute(fail)).rejects.toThrow('boom');
    expect(breaker.state).toBe('closed');

    await expect(breaker.execute(fail)).rejects.toThrow('boom');
    expect(breaker.state).toBe('open');
    await expect(breaker.execute(async () => 'ok')).rejects.toBeInstanceOf(CircuitOpenError);
  });

  it('allows a single half-open probe and closes on success', async () => {
    const { breaker, advance } = createBreaker();
    await expect(breaker.execute(fail)).rejects.toThrow();
    await expect(breaker.execute(fail)).rejects.toThrow();
    advance(1_000);

    let releaseProbe: (value: string) => void = () => undefined;
    const probe = breaker.execute(() => new Promise<string>((resolve) => (releaseProbe = resolve)));
    expect(breaker.state).toBe('half-open');
    await expect(breaker.execute(async () => 'concurrent')).rejects.toBeInstanceOf(
      CircuitOpenError,
    );

    releaseProbe('ok');
    await expect(probe).resolves.toBe('ok');
    expect(breaker.state).toBe('closed');
  });

  it('reopens immediately when the half-open probe fails', async () => {
    const { breaker, advance } = createBreaker();
    await expect(breaker.execute(fail)).rejects.toThrow();
    await expect(breaker.execute(fail)).rejects.toThrow();
    advance(1_000);

    await expect(breaker.execute(fail)).rejects.toThrow('boom');
    expect(breaker.state).toBe('open');
    await expect(breaker.execute(async () => 'ok')).rejects.toBeInstanceOf(CircuitOpenError);
  });
});
