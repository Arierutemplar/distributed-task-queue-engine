export class TaskTimeoutError extends Error {
  constructor(readonly timeoutMs: number) {
    super(`Task exceeded the ${timeoutMs}ms execution timeout`);
    this.name = 'TaskTimeoutError';
  }
}

/**
 * Runs `operation` with an AbortSignal that fires after `timeoutMs`.
 * The returned promise rejects with TaskTimeoutError even if the operation ignores the signal.
 */
export const withTimeout = async <T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number,
): Promise<T> => {
  const controller = new AbortController();
  let timer: NodeJS.Timeout | undefined;

  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      const error = new TaskTimeoutError(timeoutMs);
      controller.abort(error);
      reject(error);
    }, timeoutMs);
  });

  try {
    return await Promise.race([operation(controller.signal), timeout]);
  } finally {
    clearTimeout(timer);
  }
};
