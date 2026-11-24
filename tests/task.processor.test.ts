import { createHash } from 'node:crypto';
import type { Job } from 'bullmq';
import pino from 'pino';
import { describe, expect, it, vi } from 'vitest';
import { CircuitBreaker, CircuitOpenError } from '../src/resilience/circuit-breaker.js';
import { TaskTimeoutError } from '../src/resilience/timeout.js';
import {
  BillingMockService,
  BillingServiceException,
  type BillingGateway,
} from '../src/services/billing.mock.service.js';
import type { TaskJobData, TaskResult, TaskType } from '../src/types/task.types.js';
import { createTaskProcessor, type TaskJob } from '../src/workers/task.processor.js';

const silentLogger = pino({ level: 'silent' });

const invoice = {
  type: 'PROCESS_INVOICE',
  customerId: 'customer-1',
  amount: 10,
  currency: 'USD',
  taxId: 'TAX-1',
  items: [{ description: 'Item', quantity: 1, unitPrice: 10 }],
} as const;

const fakeJob = (data: unknown, attemptsMade = 0): TaskJob =>
  ({
    id: '1',
    name: (data as { type?: TaskType }).type ?? 'PROCESS_DOCUMENT',
    data,
    attemptsMade,
    timestamp: Date.now(),
  }) as unknown as Job<TaskJobData, TaskResult, TaskType>;

const buildProcessor = (
  billing: BillingGateway,
  overrides: { timeoutMs?: number; threshold?: number } = {},
) =>
  createTaskProcessor({
    billing,
    billingCircuit: new CircuitBreaker({
      name: 'billing',
      failureThreshold: overrides.threshold ?? 5,
      resetTimeoutMs: 60_000,
    }),
    timeoutMs: overrides.timeoutMs ?? 5_000,
    logger: silentLogger,
  });

const instantBilling = (failureRate: number): BillingMockService =>
  new BillingMockService({ failureRate, minLatencyMs: 0, maxLatencyMs: 0, random: () => 0.5 });

describe('task processor', () => {
  it('computes document statistics and a SHA-256, ignoring job metadata', async () => {
    const processor = buildProcessor(instantBilling(0));

    const content = '  héllo   world  ';
    const result = await processor(
      fakeJob({
        type: 'PROCESS_DOCUMENT',
        documentId: 'doc-1',
        content,
        meta: { requestId: 'r', payloadHash: 'h', acceptedAt: 'a' },
      }),
    );

    expect(result).toMatchObject({
      taskType: 'PROCESS_DOCUMENT',
      documentId: 'doc-1',
      characterCount: 17,
      wordCount: 2,
      sha256: createHash('sha256').update(content, 'utf8').digest('hex'),
    });
  });

  it('returns a processed notification result', async () => {
    const processor = buildProcessor(instantBilling(0));
    const result = await processor(
      fakeJob({
        type: 'SEND_NOTIFICATION',
        recipient: 'a@example.com',
        subject: 's',
        message: 'm',
      }),
    );
    expect(result).toMatchObject({
      taskType: 'SEND_NOTIFICATION',
      notificationId: '1',
      status: 'processed',
    });
  });

  it('returns a PAID invoice when billing succeeds', async () => {
    const result = await buildProcessor(instantBilling(0))(fakeJob(invoice));
    expect(result).toMatchObject({ taskType: 'PROCESS_INVOICE', status: 'PAID' });
  });

  it('rethrows the billing exception so BullMQ applies backoff', async () => {
    await expect(buildProcessor(instantBilling(1))(fakeJob(invoice))).rejects.toBeInstanceOf(
      BillingServiceException,
    );
  });

  it('fails malformed jobs with UnrecoverableError so they are not retried', async () => {
    const processor = buildProcessor(instantBilling(0));
    await expect(processor(fakeJob({ type: 'PROCESS_INVOICE', amount: -1 }))).rejects.toMatchObject(
      {
        name: 'UnrecoverableError',
        message: expect.stringContaining('Invalid task payload'),
      },
    );
  });

  it('aborts a handler that exceeds the job timeout', async () => {
    let receivedSignal: AbortSignal | undefined;
    const hangingBilling: BillingGateway = {
      processInvoice: (_invoice, signal) => {
        receivedSignal = signal;
        return new Promise(() => undefined);
      },
    };

    await expect(
      buildProcessor(hangingBilling, { timeoutMs: 20 })(fakeJob(invoice)),
    ).rejects.toBeInstanceOf(TaskTimeoutError);
    expect(receivedSignal?.aborted).toBe(true);
  });

  it('short-circuits billing once the breaker opens', async () => {
    const processInvoice = vi.fn(async () => {
      throw new BillingServiceException();
    });
    const processor = buildProcessor({ processInvoice }, { threshold: 2 });

    await expect(processor(fakeJob(invoice))).rejects.toBeInstanceOf(BillingServiceException);
    await expect(processor(fakeJob(invoice))).rejects.toBeInstanceOf(BillingServiceException);
    await expect(processor(fakeJob(invoice))).rejects.toBeInstanceOf(CircuitOpenError);
    expect(processInvoice).toHaveBeenCalledTimes(2);
  });
});
