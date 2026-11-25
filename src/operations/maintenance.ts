import type { Logger } from 'pino';

export interface MaintenanceLoop {
  run: () => Promise<void>;
  close: () => Promise<void>;
}

export const createMaintenanceLoop = (
  name: string,
  intervalMs: number,
  operation: () => Promise<void>,
  logger: Logger,
): MaintenanceLoop => {
  let active: Promise<void> | undefined;
  let closed = false;
  const run = (): Promise<void> => {
    if (closed) return Promise.resolve();
    if (active) return active;
    active = operation()
      .catch((error: unknown) => {
        logger.error({ err: error, operation: name }, 'Maintenance pass failed');
      })
      .finally(() => {
        active = undefined;
      });
    return active;
  };
  const timer = setInterval(() => void run(), intervalMs);
  timer.unref();
  return {
    run,
    async close() {
      closed = true;
      clearInterval(timer);
      await active;
    },
  };
};
