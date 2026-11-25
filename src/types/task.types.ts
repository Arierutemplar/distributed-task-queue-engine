import { z } from 'zod';

const schemaVersion = z
  .literal(1, { error: 'Unsupported schemaVersion; supported version: 1' })
  .optional();

export const processDocumentPayloadSchema = z.object({
  type: z.literal('PROCESS_DOCUMENT'),
  schemaVersion,
  documentId: z.string().trim().min(1).max(200),
  content: z.string().min(1).max(1_000_000),
});

export const sendNotificationPayloadSchema = z.object({
  type: z.literal('SEND_NOTIFICATION'),
  schemaVersion,
  recipient: z.email().max(320),
  subject: z.string().trim().min(1).max(200),
  message: z.string().min(1).max(10_000),
});

export const processInvoicePayloadSchema = z.object({
  type: z.literal('PROCESS_INVOICE'),
  schemaVersion,
  customerId: z.string().trim().min(1).max(200),
  amount: z.number().positive(),
  currency: z.string().trim().min(1).max(10),
  taxId: z.string().trim().min(1).max(100),
  items: z
    .array(
      z.object({
        description: z.string().trim().min(1).max(500),
        quantity: z.number().int().positive(),
        unitPrice: z.number().positive(),
      }),
    )
    .min(1),
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

export const taskTypeSchema = z.enum(['PROCESS_DOCUMENT', 'SEND_NOTIFICATION', 'PROCESS_INVOICE']);

// Printable ASCII without spaces, matching the IETF Idempotency-Key header draft.
export const idempotencyKeySchema = z
  .string()
  .min(1)
  .max(255)
  .regex(/^[\x21-\x7E]+$/u, 'must be printable ASCII without spaces');

export const taskIdSchema = z
  .string()
  .min(1)
  .max(200)
  .regex(/^[A-Za-z0-9_-]+$/u, 'must contain only letters, digits, "-" or "_"');

// Owner of jobs created while API_KEYS is unset (development) or before tenants existed.
export const PUBLIC_TENANT_ID = 'public';

export interface TaskJobMeta {
  requestId: string;
  tenantId?: string;
  payloadHash: string;
  acceptedAt: string;
  idempotencyKey?: string;
  replayOf?: string;
  /** W3C trace context of the request that accepted the task, when tracing is enabled. */
  traceparent?: string;
  tracestate?: string;
}

export type TaskJobData = TaskPayload & { meta?: TaskJobMeta };

export const taskAcceptedResponseSchema = z.object({
  taskId: z.string().min(1),
  taskType: taskTypeSchema,
  status: z.literal('accepted'),
  acceptedAt: z.iso.datetime(),
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
  completedAt: z.iso.datetime(),
});

export const notificationTaskResultSchema = z.object({
  taskType: z.literal('SEND_NOTIFICATION'),
  notificationId: z.string().min(1),
  status: z.literal('processed'),
  processedAt: z.iso.datetime(),
});

export const invoiceTaskResultSchema = z.object({
  taskType: z.literal('PROCESS_INVOICE'),
  invoiceId: z.uuid(),
  status: z.literal('PAID'),
  pdfUrl: z.url(),
  processedAt: z.iso.datetime(),
});

export const taskResultSchema = z.discriminatedUnion('taskType', [
  documentTaskResultSchema,
  notificationTaskResultSchema,
  invoiceTaskResultSchema,
]);

export type TaskResult = z.infer<typeof taskResultSchema>;
export type InvoiceTaskResult = z.infer<typeof invoiceTaskResultSchema>;

export type TaskState =
  | 'waiting'
  | 'waiting-children'
  | 'prioritized'
  | 'active'
  | 'delayed'
  | 'completed'
  | 'failed'
  | 'dead-lettered'
  | 'unknown';

export interface TaskStatusResponse {
  taskId: string;
  taskType: TaskType;
  state: TaskState;
  attemptsMade: number;
  maxAttempts: number;
  acceptedAt: string | null;
  processedAt: string | null;
  finishedAt: string | null;
  result: TaskResult | null;
  failedReason: string | null;
}

export interface DeadLetterJobData {
  originalJobId: string;
  taskType: TaskType;
  data: TaskJobData;
  failedReason: string;
  attemptsMade: number;
  failedAt: string;
}
