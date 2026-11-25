import { Writable } from 'node:stream';
import { context, propagation, trace } from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import pino from 'pino';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({ add: vi.fn(), addDeadLetter: vi.fn() }));

vi.mock('../src/config/redis.js', () => ({ redis: {}, pingRedis: vi.fn() }));
vi.mock('../src/queues/task.queue.js', () => ({
  TASK_QUEUE_NAME: 'tasks',
  TASK_MAX_ATTEMPTS: 5,
  taskQueue: {
    name: 'tasks',
    add: mocks.add,
    getJob: vi.fn(),
    getJobCounts: vi.fn(async () => ({})),
  },
}));
vi.mock('../src/queues/dead-letter.queue.js', () => ({
  addDeadLetter: mocks.addDeadLetter,
  readDeadLetterCounts: vi.fn(async () => ({})),
  deadLetterJobId: (id: string) => `dlq-${id}`,
  deadLetterQueue: {
    name: 'tasks-dead-letter',
    getJob: vi.fn(),
    getJobCounts: vi.fn(async () => ({})),
  },
}));
vi.mock('../src/routes/board.routes.js', async () => {
  const { Router } = await import('express');
  return { boardRouter: Router() };
});

import { createApp } from '../src/app.js';
import { loadConfig } from '../src/config/env.js';
import { createAuditLog } from '../src/observability/audit.js';
import {
  activeTraceFields,
  currentTraceContext,
  initTracing,
  jobAttemptContext,
  withTraceContext,
} from '../src/observability/tracing.js';
import { RedisCircuitBreaker } from '../src/resilience/redis-circuit-breaker.js';
import type { TaskJobMeta } from '../src/types/task.types.js';
import { createTaskProcessor, type TaskJob } from '../src/workers/task.processor.js';
import { routeToDeadLetter } from '../src/workers/task.worker.js';

describe('distributed tracing', () => {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  const contextManager = new AsyncLocalStorageContextManager();

  beforeAll(() => {
    context.setGlobalContextManager(contextManager.enable());
    propagation.setGlobalPropagator(new W3CTraceContextPropagator());
    trace.setGlobalTracerProvider(provider);
  });

  afterAll(async () => {
    await provider.shutdown();
    trace.disable();
    propagation.disable();
    context.disable();
  });

  it('continues the inbound trace through the job meta into the worker span', async () => {
    mocks.add.mockResolvedValue({ id: 'job-1' });
    const key = 'k'.repeat(32);
    const app = await createApp({
      config: loadConfig({ NODE_ENV: 'test', LOG_LEVEL: 'silent', API_KEYS: `acme:${key}` }),
    });
    const traceId = '4bf92f3577b34da6a3ce929d0e0e4736';

    const response = await request(app)
      .post('/api/v1/tasks/process')
      .set('traceparent', `00-${traceId}-00f067aa0ba902b7-01`)
      .set('tracestate', 'vendor=value')
      .set('X-API-Key', key)
      .set('X-Request-Id', 'trace-request')
      .send({ type: 'PROCESS_DOCUMENT', documentId: 'd-1', content: 'traced' });
    expect(response.status).toBe(202);

    const meta = (mocks.add.mock.calls[0]?.[1] as { meta: TaskJobMeta }).meta;
    expect(meta.traceparent).toMatch(new RegExp(`^00-${traceId}-[0-9a-f]{16}-01$`, 'u'));
    expect(meta.tracestate).toBe('vendor=value');

    const lines: string[] = [];
    const sink = new Writable({
      write(chunk: Buffer, _encoding, callback) {
        lines.push(chunk.toString());
        callback();
      },
    });
    const processor = createTaskProcessor({
      logger: pino({ mixin: activeTraceFields }, sink),
      timeoutMs: 1_000,
      billing: { processInvoice: vi.fn() },
      billingCircuit: { execute: async (operation) => operation() },
      effects: { begin: async () => undefined, complete: async (_key, _hash, result) => result },
      effectNamespace: 'test',
    });
    const data = mocks.add.mock.calls[0]?.[1] as TaskJob['data'];
    const job = {
      id: 'job-1',
      name: 'PROCESS_DOCUMENT',
      attemptsMade: 0,
      data,
    } as unknown as TaskJob;
    await processor(job);
    mocks.addDeadLetter.mockResolvedValueOnce(true);
    await routeToDeadLetter(job, new Error('test failure'));

    const spans = exporter.getFinishedSpans();
    const server = spans.find((span) => span.name === 'POST /api/v1/tasks/process');
    const producer = spans.find((span) => span.name === 'enqueue task');
    const consumer = spans.find((span) => span.name === 'process PROCESS_DOCUMENT');
    const deadLetter = spans.find((span) => span.name === 'dead-letter.route');
    expect(server?.spanContext().traceId).toBe(traceId);
    expect(server?.attributes['http.response.status_code']).toBe(202);
    expect(consumer?.spanContext().traceId).toBe(traceId);
    expect(producer?.parentSpanContext?.spanId).toBe(server?.spanContext().spanId);
    expect(consumer?.parentSpanContext?.spanId).toBe(producer?.spanContext().spanId);
    expect(deadLetter?.parentSpanContext?.spanId).toBe(consumer?.spanContext().spanId);
    expect(deadLetter?.attributes['dead_letter.routed']).toBe(true);
    expect(server?.attributes).toMatchObject({
      'request.id': 'trace-request',
      'tenant.id': 'acme',
      'task.id': 'job-1',
    });
    expect(consumer?.attributes).toMatchObject({
      'request.id': 'trace-request',
      'tenant.id': 'acme',
      'task.id': 'job-1',
      'task.type': 'PROCESS_DOCUMENT',
    });
    expect(JSON.parse(lines[0] ?? '{}')).toMatchObject({
      requestId: 'trace-request',
      traceId,
      spanId: consumer?.spanContext().spanId,
    });
    expect(withTraceContext(jobAttemptContext(job), activeTraceFields)).toEqual({
      traceId,
      spanId: consumer?.spanContext().spanId,
    });
  });

  it('records the shared circuit state and a child span for billing', async () => {
    const breaker = new RedisCircuitBreaker({
      redis: {
        eval: vi
          .fn()
          .mockResolvedValueOnce(['closed', 'generation', 0, 1])
          .mockResolvedValueOnce('closed'),
      },
      key: 'test',
      name: 'billing',
      failureThreshold: 1,
      resetTimeoutMs: 1_000,
      probeLeaseMs: 1_000,
    });
    const processor = createTaskProcessor({
      logger: pino({ level: 'silent' }),
      timeoutMs: 1_000,
      billingCircuit: breaker,
      effects: { begin: async () => undefined, complete: async (_key, _hash, result) => result },
      effectNamespace: 'test',
      billing: {
        processInvoice: async () => ({
          taskType: 'PROCESS_INVOICE',
          invoiceId: '6a4b3f1e-1d2c-4b5a-8c7d-9e0f1a2b3c4d',
          status: 'PAID',
          pdfUrl: 'https://billing.example.test/invoice',
          processedAt: '2026-01-01T00:00:00.000Z',
        }),
      },
    });
    await processor({
      id: 'invoice-1',
      name: 'PROCESS_INVOICE',
      attemptsMade: 0,
      data: {
        type: 'PROCESS_INVOICE',
        customerId: 'customer',
        amount: 10,
        currency: 'USD',
        taxId: 'tax',
        items: [{ description: 'item', quantity: 1, unitPrice: 10 }],
      },
    } as unknown as TaskJob);
    const spans = exporter.getFinishedSpans();
    const consumer = spans.find((span) => span.name === 'process PROCESS_INVOICE');
    const billing = spans.find((span) => span.name === 'billing.processInvoice');
    expect(consumer?.attributes['circuit_breaker.state']).toBe('closed');
    expect(billing?.parentSpanContext?.spanId).toBe(consumer?.spanContext().spanId);
  });
});

describe('tracing disabled', () => {
  it('does not initialize the SDK without an endpoint or when disabled', async () => {
    await expect(
      initTracing({ enabled: true, serviceName: 'test', role: 'worker' }),
    ).resolves.toBeUndefined();
    await expect(
      initTracing({
        enabled: false,
        endpoint: 'http://localhost:4318',
        serviceName: 'test',
        role: 'worker',
      }),
    ).resolves.toBeUndefined();
    expect(activeTraceFields()).toEqual({});
    expect(currentTraceContext()).toEqual({});
  });
});

describe('audit log', () => {
  it('emits one structured, secret-free line per record and skips persistence without Redis', async () => {
    const lines: string[] = [];
    const sink = new Writable({
      write(chunk: Buffer, _encoding, callback) {
        lines.push(chunk.toString());
        callback();
      },
    });
    const audit = createAuditLog({
      logger: pino({ level: 'info' }, sink),
      now: () => new Date('2026-01-01T00:00:00.000Z'),
    });

    await audit.record({
      action: 'dead_letter.replay',
      outcome: 'success',
      actor: 'admin',
      requestId: 'req-1',
      method: 'POST',
      path: '/admin/api/dead-letter/9/replay',
      tenantId: 'acme',
      taskId: '9',
      newTaskId: '10',
    });

    expect(lines).toHaveLength(1);
    const line = JSON.parse(lines[0] ?? '{}') as Record<string, unknown>;
    expect(line).toMatchObject({
      audit: true,
      level: 30,
      event: {
        timestamp: '2026-01-01T00:00:00.000Z',
        action: 'dead_letter.replay',
        actor: 'admin',
        requestId: 'req-1',
        tenantId: 'acme',
        taskId: '9',
        newTaskId: '10',
        outcome: 'success',
      },
    });
    await expect(audit.list(10)).resolves.toEqual([]);
  });

  it('persists to a capped stream, lists newest first, and survives Redis errors', async () => {
    const stored: [string, string[]][] = [];
    const redis = {
      xadd: vi.fn(async (...args: (string | number)[]) => {
        stored.unshift([`${stored.length + 1}-0`, ['record', String(args.at(-1))]]);
        return 'id';
      }),
      xrevrange: vi.fn(async () => [
        ...stored,
        ['0-0', ['record', '{not json']],
        ['0-1', ['other', 'x']],
      ]),
    };
    const logger = pino({ level: 'silent' });
    const sink = {
      write: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const audit = createAuditLog({
      logger,
      sink,
      redis: redis as never,
      streamKey: 'svc:audit:admin',
      maxEntries: 50,
    });
    const event = {
      action: 'dead_letter.delete',
      outcome: 'success',
      actor: 'admin',
      requestId: 'req-2',
      method: 'DELETE',
      path: '/admin/api/dead-letter/7',
      taskId: '7',
    } as const;

    await audit.record(event);
    expect(redis.xadd).toHaveBeenCalledWith(
      'svc:audit:admin',
      'MAXLEN',
      '~',
      50,
      '*',
      'record',
      expect.any(String),
    );
    // Malformed or foreign entries are skipped instead of failing the endpoint.
    await expect(audit.list(5)).resolves.toEqual([
      expect.objectContaining({ ...event, id: '1-0' }),
    ]);
    expect(redis.xrevrange).toHaveBeenCalledWith('svc:audit:admin', '+', '-', 'COUNT', 5);

    redis.xadd.mockRejectedValueOnce(new Error('READONLY'));
    await expect(audit.record(event)).resolves.toBeUndefined();
    expect(sink.write).toHaveBeenCalledTimes(2);
    expect(sink.write).toHaveBeenLastCalledWith(expect.objectContaining(event));
  });
});
