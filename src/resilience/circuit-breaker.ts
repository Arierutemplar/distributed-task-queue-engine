export type CircuitState = 'closed' | 'open' | 'half-open';

export class CircuitOpenError extends Error {
  constructor(
    readonly circuitName: string,
    readonly retryAfterMs: number,
  ) {
    super(`Circuit "${circuitName}" is open; retry after ${retryAfterMs}ms`);
    this.name = 'CircuitOpenError';
  }
}

export interface CircuitBreakerOptions {
  name: string;
  failureThreshold: number;
  resetTimeoutMs: number;
  now?: () => number;
  onStateChange?: (state: CircuitState, previous: CircuitState) => void;
}

/**
 * Per-process breaker: closed -> open after `failureThreshold` consecutive failures,
 * open -> half-open after `resetTimeoutMs`, where a single probe decides the next state.
 */
export class CircuitBreaker {
  private currentState: CircuitState = 'closed';
  private consecutiveFailures = 0;
  private openedAt = 0;
  private probeInFlight = false;
  private readonly now: () => number;

  constructor(private readonly options: CircuitBreakerOptions) {
    this.now = options.now ?? Date.now;
  }

  get state(): CircuitState {
    return this.currentState;
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    let isProbe = false;

    if (this.currentState === 'open') {
      const elapsed = this.now() - this.openedAt;
      if (elapsed < this.options.resetTimeoutMs) {
        throw new CircuitOpenError(this.options.name, this.options.resetTimeoutMs - elapsed);
      }
      this.transition('half-open');
    }

    if (this.currentState === 'half-open') {
      if (this.probeInFlight) {
        throw new CircuitOpenError(this.options.name, this.options.resetTimeoutMs);
      }
      this.probeInFlight = true;
      isProbe = true;
    }

    try {
      const result = await operation();
      this.consecutiveFailures = 0;
      if (this.currentState !== 'closed') {
        this.transition('closed');
      }
      return result;
    } catch (error: unknown) {
      this.consecutiveFailures += 1;
      if (
        this.currentState === 'half-open' ||
        this.consecutiveFailures >= this.options.failureThreshold
      ) {
        this.openedAt = this.now();
        if (this.currentState !== 'open') {
          this.transition('open');
        }
      }
      throw error;
    } finally {
      if (isProbe) {
        this.probeInFlight = false;
      }
    }
  }

  private transition(next: CircuitState): void {
    const previous = this.currentState;
    this.currentState = next;
    this.options.onStateChange?.(next, previous);
  }
}
