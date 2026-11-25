import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';
import { config } from '../config/env.js';
import { deadLetterQueue, readDeadLetterCounts } from '../queues/dead-letter.queue.js';
import { taskQueue } from '../queues/task.queue.js';
import type { CircuitState } from '../resilience/circuit-breaker.js';

export const metricsRegistry = new Registry();
metricsRegistry.setDefaultLabels({ service: config.SERVICE_NAME, role: config.PROCESS_ROLE });
collectDefaultMetrics({ register: metricsRegistry });

export const httpRequestDuration = new Histogram({
  name: 'http_request_duration_seconds',
  help: 'HTTP request latency by method, route and status code',
  labelNames: ['method', 'route', 'status_code'] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [metricsRegistry],
});

export const tasksEnqueuedTotal = new Counter({
  name: 'tasks_enqueued_total',
  help: 'Tasks accepted by the API, split by idempotent replays',
  labelNames: ['task_type', 'replayed'] as const,
  registers: [metricsRegistry],
});

export const tasksProcessedTotal = new Counter({
  name: 'tasks_processed_total',
  help: 'Task attempts finished by this worker, by outcome',
  labelNames: ['task_type', 'outcome'] as const,
  registers: [metricsRegistry],
});

export const taskDuration = new Histogram({
  name: 'task_processing_duration_seconds',
  help: 'Duration of a single task attempt',
  labelNames: ['task_type', 'outcome'] as const,
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 3, 5, 10, 30],
  registers: [metricsRegistry],
});

export const tasksDeadLetteredTotal = new Counter({
  name: 'tasks_dead_lettered_total',
  help: 'Tasks moved to the dead-letter queue after their final failure',
  labelNames: ['task_type'] as const,
  registers: [metricsRegistry],
  async collect() {
    const counts = await readDeadLetterCounts();
    this.reset();
    for (const [taskType, value] of Object.entries(counts)) {
      const count = Number(value);
      if (!Number.isSafeInteger(count) || count < 0) {
        throw new Error(`Invalid dead-letter count for ${taskType}`);
      }
      this.inc({ task_type: taskType }, count);
    }
  },
});

const circuitStateValue: Record<CircuitState, number> = { closed: 0, 'half-open': 1, open: 2 };

export const circuitBreakerState = new Gauge({
  name: 'circuit_breaker_state',
  help: 'Circuit breaker state: 0 closed, 1 half-open, 2 open',
  labelNames: ['circuit'] as const,
  registers: [metricsRegistry],
});

export const setCircuitState = (circuit: string, state: CircuitState): void => {
  circuitBreakerState.set({ circuit }, circuitStateValue[state]);
};

const queueStates = [
  'waiting',
  'active',
  'delayed',
  'completed',
  'failed',
  'paused',
  'prioritized',
] as const;

new Gauge({
  name: 'queue_jobs',
  help: 'Jobs per BullMQ queue and state, read from Redis at scrape time',
  labelNames: ['queue', 'state'] as const,
  registers: [metricsRegistry],
  async collect() {
    this.reset();
    const [taskCounts, deadLetterCounts] = await Promise.all([
      taskQueue.getJobCounts(...queueStates),
      deadLetterQueue.getJobCounts('waiting'),
    ]);
    for (const state of queueStates) {
      this.set({ queue: taskQueue.name, state }, taskCounts[state] ?? 0);
    }
    // Counts are typed as always present, but BullMQ omits states it could not read.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    this.set({ queue: deadLetterQueue.name, state: 'waiting' }, deadLetterCounts.waiting ?? 0);
  },
});
