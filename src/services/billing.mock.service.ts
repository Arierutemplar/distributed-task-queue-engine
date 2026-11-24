import { randomUUID } from 'node:crypto';
import type { InvoiceTaskResult, ProcessInvoicePayload } from '../types/task.types.js';

export class BillingServiceException extends Error {
  readonly statusCode = 503;

  constructor(message = 'Billing service is temporarily unavailable') {
    super(message);
    this.name = 'BillingServiceException';
  }
}

export interface BillingGateway {
  processInvoice(invoice: ProcessInvoicePayload, signal?: AbortSignal): Promise<InvoiceTaskResult>;
}

export interface BillingMockOptions {
  failureRate?: number;
  minLatencyMs?: number;
  maxLatencyMs?: number;
  random?: () => number;
}

const abortReason = (signal?: AbortSignal): Error =>
  signal?.reason instanceof Error ? signal.reason : new Error('Operation aborted');

const abortableDelay = (delayMs: number, signal?: AbortSignal): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(abortReason(signal));
      return;
    }
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(abortReason(signal));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, delayMs);
    signal?.addEventListener('abort', onAbort, { once: true });
  });

export class BillingMockService implements BillingGateway {
  private readonly failureRate: number;
  private readonly minLatencyMs: number;
  private readonly maxLatencyMs: number;
  private readonly random: () => number;

  constructor(options: BillingMockOptions = {}) {
    this.failureRate = options.failureRate ?? 0.2;
    this.minLatencyMs = options.minLatencyMs ?? 1_500;
    this.maxLatencyMs = options.maxLatencyMs ?? 3_000;
    this.random = options.random ?? Math.random;
  }

  async processInvoice(
    _invoice: ProcessInvoicePayload,
    signal?: AbortSignal,
  ): Promise<InvoiceTaskResult> {
    const delayMs = this.minLatencyMs + this.random() * (this.maxLatencyMs - this.minLatencyMs);
    await abortableDelay(delayMs, signal);

    if (this.random() < this.failureRate) {
      throw new BillingServiceException();
    }

    const invoiceId = randomUUID();
    return {
      taskType: 'PROCESS_INVOICE',
      invoiceId,
      status: 'PAID',
      pdfUrl: `https://billing.example.test/invoices/${invoiceId}.pdf`,
      processedAt: new Date().toISOString(),
    };
  }
}
