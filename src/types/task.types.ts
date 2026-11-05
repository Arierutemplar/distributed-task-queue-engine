import { z } from 'zod';

export const processDocumentPayloadSchema = z.object({
  type: z.literal('PROCESS_DOCUMENT'),
  documentId: z.string().trim().min(1).max(200),
  content: z.string().min(1).max(1_000_000),
});

export const sendNotificationPayloadSchema = z.object({
  type: z.literal('SEND_NOTIFICATION'),
  recipient: z.string().email().max(320),
  subject: z.string().trim().min(1).max(200),
  message: z.string().min(1).max(10_000),
});

export const processInvoicePayloadSchema = z.object({
  type: z.literal('PROCESS_INVOICE'),
  customerId: z.string().trim().min(1).max(200),
  amount: z.number().positive(),
  currency: z.string().trim().min(1).max(10),
  taxId: z.string().trim().min(1).max(100),
  items: z.array(
    z.object({
      description: z.string().trim().min(1).max(500),
      quantity: z.number().int().positive(),
      unitPrice: z.number().positive(),
    }),
  ).min(1),
});

export const taskPayloadSchema = z.discriminatedUnion('type', [
  processDocumentPayloadSchema,
  sendNotificationPayloadSchema,
  processInvoicePayloadSchema,
]);

export type ProcessDocumentPayload = z.infer<typeof processDocumentPayloadSchema>;
export type SendNotificationPayload = z.infer<typeof sendNotificationPayloadSchema>;
export type ProcessInvoicePayload = z.infer<typeof processInvoicePayloadSchema>;
export type TaskPayload = z.infer<typeof taskPayloadSchema>;
export type TaskType = TaskPayload['type'];

export const taskAcceptedResponseSchema = z.object({
  taskId: z.string().min(1),
  taskType: z.enum(['PROCESS_DOCUMENT', 'SEND_NOTIFICATION', 'PROCESS_INVOICE']),
  status: z.literal('accepted'),
  acceptedAt: z.string().datetime(),
});

export type TaskAcceptedResponse = z.infer<typeof taskAcceptedResponseSchema>;

export const taskErrorResponseSchema = z.object({
  error: z.string().min(1),
});

export type TaskErrorResponse = z.infer<typeof taskErrorResponseSchema>;

export const documentTaskResultSchema = z.object({
  taskType: z.literal('PROCESS_DOCUMENT'),
  documentId: z.string(),
  characterCount: z.number().int().nonnegative(),
  wordCount: z.number().int().nonnegative(),
  sha256: z.string().length(64),
  completedAt: z.string().datetime(),
});

export const notificationTaskResultSchema = z.object({
  taskType: z.literal('SEND_NOTIFICATION'),
  notificationId: z.string().min(1),
  status: z.literal('processed'),
  processedAt: z.string().datetime(),
});

export const invoiceTaskResultSchema = z.object({
  taskType: z.literal('PROCESS_INVOICE'),
  invoiceId: z.string().uuid(),
  status: z.literal('PAID'),
  pdfUrl: z.string().url(),
  processedAt: z.string().datetime(),
});

export const taskResultSchema = z.discriminatedUnion('taskType', [
  documentTaskResultSchema,
  notificationTaskResultSchema,
  invoiceTaskResultSchema,
]);

export type TaskResult = z.infer<typeof taskResultSchema>;
export type InvoiceTaskResult = z.infer<typeof invoiceTaskResultSchema>;
