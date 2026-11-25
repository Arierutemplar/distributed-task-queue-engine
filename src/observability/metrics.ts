import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';
import { config } from '../config/env.js';
import { deadLetterQueue, readDeadLetterCounts } from '../queues/dead-letter.queue.js';
import { taskQueue } from '../queues/task.queue.js';
import type { CircuitState } from '../resilience/circuit-breaker.js';
import { readTenantInflight } from '../queues/admission.js';
import { readTenantActiveSlots } from '../queues/tenant-slots.js';

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

export const backpressureRejections = new Counter({
  name: 'task_backpressure_rejections_total',
  help: 'Admission rejections by capacity limit',
  labelNames: ['reason'] as const,
  registers: [metricsRegistry],
});

export const fairShareDeferrals = new Counter({
  name: 'tenant_fair_share_deferrals_total',
  help: 'Task attempts deferred because their tenant has no free active slot',
  labelNames: ['tenant'] as const,
  registers: [metricsRegistry],
});

new Gauge({
  name: 'tenant_active_slots',
  help: 'Current unexpired worker slot leases per tenant',
  labelNames: ['tenant'] as const,
  registers: [metricsRegistry],
  async collect() {
    const counts = await readTenantActiveSlots();
    this.reset();
    for (const [tenant, count] of counts) this.set({ tenant }, count);
  },
});

new Gauge({
  name: 'tenant_tasks_inflight',
  help: 'Current waiting, active and delayed tasks per tenant from Redis',
  labelNames: ['tenant'] as const,
  registers: [metricsRegistry],
  async collect() {
    const counts = await readTenantInflight();
    this.reset();
    for (const [tenant, count] of counts) this.set({ tenant }, count);
  },
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

export const taskEndToEndDuration = new Histogram({
  name: 'task_end_to_end_duration_seconds',
  help: 'Time from job creation to terminal outcome, including queue wait and retries',
  labelNames: ['task_type', 'outcome'] as const,
  buckets: [0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 300],
  registers: [metricsRegistry],
});
export const auditDeliveryTotal = new Counter({
  name: 'audit_delivery_total',
  help: 'Durable audit enqueue/delivery outcomes',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});
export const auditOutboxJobs = new Gauge({
  name: 'audit_outbox_jobs',
  help: 'Last observed durable audit backlog by state',
  labelNames: ['state'] as const,
  registers: [metricsRegistry],
});
export const auditOutboxOldestAge = new Gauge({
  name: 'audit_outbox_oldest_age_seconds',
  help: 'Oldest observed head age across pending audit states (sample, not exhaustive)',
  registers: [metricsRegistry],
});
export const dlqReconciledTotal = new Counter({
  name: 'dlq_reconciled_total',
  help: 'Missing DLQ entries recovered by this reconciler',
  registers: [metricsRegistry],
});
export const maintenanceLastSuccess = new Gauge({
  name: 'maintenance_last_success_timestamp_seconds',
  help: 'Last successful maintenance pass',
  labelNames: ['operation'] as const,
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
