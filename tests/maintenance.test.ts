import pino from 'pino';
import { afterEach, expect, it, vi } from 'vitest';
import { createMaintenanceLoop } from '../src/operations/maintenance.js';

afterEach(() => vi.useRealTimers());
it('shares in-flight work, drains on close and refuses later runs', async () => {
  vi.useFakeTimers();
  let complete: (() => void) | undefined;
  const operation = vi.fn(
    () =>
      new Promise<void>((resolve) => {
        complete = resolve;
      }),
  );
  const loop = createMaintenanceLoop('test', 100, operation, pino({ level: 'silent' }));
  const first = loop.run();
  expect(loop.run()).toBe(first);
  await vi.advanceTimersByTimeAsync(300);
  expect(operation).toHaveBeenCalledTimes(1);
  const closing = loop.close();
  complete?.();
  await closing;
  await loop.run();
  expect(operation).toHaveBeenCalledTimes(1);
});
it('logs failed passes and allows a later recovery', async () => {
  const logger = pino({ level: 'silent' });
  const error = vi.spyOn(logger, 'error');
  const operation = vi
    .fn()
    .mockRejectedValueOnce(new Error('failure'))
    .mockResolvedValueOnce(undefined);
  const loop = createMaintenanceLoop('test', 10000, operation, logger);
  await loop.run();
  expect(error).toHaveBeenCalled();
  await loop.run();
  expect(operation).toHaveBeenCalledTimes(2);
  await loop.close();
});
