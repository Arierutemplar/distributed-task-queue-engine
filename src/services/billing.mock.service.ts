import { randomUUID } from 'node:crypto';
import type {
  InvoiceTaskResult,
  ProcessInvoicePayload,
} from '../types/task.types.js';

export class BillingServiceException extends Error {
  readonly statusCode = 503;

  constructor(message = 'Billing service is temporarily unavailable') {
    super(message);
    this.name = 'BillingServiceException';
  }
}

export class BillingMockService {
  async processInvoice(
    _invoice: ProcessInvoicePayload,
  ): Promise<InvoiceTaskResult> {
    const delayMs = 1_500 + Math.random() * 1_500;
    await new Promise<void>((resolve) => {
      setTimeout(resolve, delayMs);
    });

    if (Math.random() < 0.2) {
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
