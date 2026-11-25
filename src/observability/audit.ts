import type { Redis } from 'ioredis';
import type { Logger } from 'pino';
import { randomUUID } from 'node:crypto';

export const AUDIT_ACTIONS = [
  'admin.auth_failed',
  'admin.auth_locked',
  'admin.disabled',
  'admin.access_denied',
  'admin.audit_read',
  'admin.secrets_reload',
  'admin.bull_board_action',
  'dead_letter.list',
  'dead_letter.replay',
  'dead_letter.delete',
] as const;
export const AUDIT_OUTCOMES = [
  'pending',
  'success',
  'denied',
  'not_found',
  'invalid',
  'error',
] as const;
export type AuditAction = (typeof AUDIT_ACTIONS)[number];
export type AuditOutcome = (typeof AUDIT_OUTCOMES)[number];

/** What the caller describes. Never put credentials, API keys or payloads here. */
export interface AuditEvent {
  action: AuditAction;
  outcome: AuditOutcome;
  /** Authenticated admin username, or null when authentication did not succeed. */
  actor: string | null;
  subject?: string | null;
  role?: string | null;
  requestId: string;
  method: string;
  path: string;
  ip?: string;
  statusCode?: number;
  tenantId?: string;
  taskId?: string;
  newTaskId?: string;
  operationId?: string;
  reason?: string;
}

export interface AuditRecord extends AuditEvent {
  id?: string;
  archiveId?: string;
  timestamp: string;
}

export interface AuditLog {
  record: (event: AuditEvent, options?: { requirePersistence?: boolean }) => Promise<void>;
  list: (limit: number) => Promise<AuditRecord[]>;
}

export interface AuditSink {
  durable?: boolean;
  /** Archives one record; failures must be logged without rejecting the main audit flow. */
  write: (record: AuditRecord) => Promise<void>;
  close: () => Promise<void>;
}

export const AUDIT_STREAM_KEY_SUFFIX = 'audit:admin';

export interface AuditLogOptions {
  logger: Logger;
  /** When set, records are also appended to a capped Redis stream so they can be queried. */
  redis?: Redis;
  streamKey?: string;
  maxEntries?: number;
  now?: () => Date;
  sink?: AuditSink;
}

/**
 * Structured audit trail for privileged actions. Every record is emitted as a log line
 * (`audit: true`) for the log pipeline/SIEM and, optionally, persisted in a Redis stream
 * capped at `maxEntries`. A required intent fails closed if configured Redis persistence fails;
 * otherwise persistence failures are logged and the record remains in structured logs.
 */
export const createAuditLog = (options: AuditLogOptions): AuditLog => {
  const log = options.logger.child({ audit: true });
  const now = options.now ?? (() => new Date());
  const streamKey = options.streamKey ?? AUDIT_STREAM_KEY_SUFFIX;
  const maxEntries = options.maxEntries ?? 10_000;
  const persist = options.redis && maxEntries > 0 ? options.redis : undefined;

  return {
    async record(event, recordOptions) {
      const record: AuditRecord = {
        timestamp: now().toISOString(),
        ...event,
        ...(options.sink?.durable ? { archiveId: randomUUID() } : {}),
      };
      const level = record.outcome === 'success' ? 'info' : 'warn';
      log[level]({ event: record }, `audit ${record.action} ${record.outcome}`);
      if (persist) {
        try {
          await persist.xadd(
            streamKey,
            'MAXLEN',
            '~',
            maxEntries,
            '*',
            'record',
            JSON.stringify(record),
          );
        } catch {
          log.error(
            { action: record.action, requestId: record.requestId },
            'Failed to persist audit record',
          );
          if (recordOptions?.requirePersistence) {
            throw new Error('Required audit persistence failed');
          }
        }
      } else if (recordOptions?.requirePersistence) {
        log.warn({ action: record.action }, 'Required audit intent is available in logs only');
      }
      await options.sink?.write(record);
    },

    async list(limit) {
      if (!persist) {
        return [];
      }
      const entries = await persist.xrevrange(streamKey, '+', '-', 'COUNT', limit);
      return entries.flatMap(([id, fields]) => {
        const index = fields.indexOf('record');
        const raw = index >= 0 ? fields[index + 1] : undefined;
        if (!raw) {
          return [];
        }
        try {
          return [{ ...(JSON.parse(raw) as AuditRecord), id }];
        } catch {
          return [];
        }
      });
    },
  };
};
