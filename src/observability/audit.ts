import type { Redis } from 'ioredis';
import type { Logger } from 'pino';

export type AuditAction =
  | 'admin.auth_failed'
  | 'admin.auth_locked'
  | 'admin.disabled'
  | 'admin.audit_read'
  | 'admin.bull_board_action'
  | 'dead_letter.list'
  | 'dead_letter.replay'
  | 'dead_letter.delete';

export type AuditOutcome = 'success' | 'denied' | 'not_found' | 'invalid' | 'error';

/** What the caller describes. Never put credentials, API keys or payloads here. */
export interface AuditEvent {
  action: AuditAction;
  outcome: AuditOutcome;
  /** Authenticated admin username, or null when authentication did not succeed. */
  actor: string | null;
  requestId: string;
  method: string;
  path: string;
  ip?: string;
  statusCode?: number;
  tenantId?: string;
  taskId?: string;
  newTaskId?: string;
  reason?: string;
}

export interface AuditRecord extends AuditEvent {
  id?: string;
  timestamp: string;
}

export interface AuditLog {
  record: (event: AuditEvent) => Promise<void>;
  list: (limit: number) => Promise<AuditRecord[]>;
}

export const AUDIT_STREAM_KEY_SUFFIX = 'audit:admin';

export interface AuditLogOptions {
  logger: Logger;
  /** When set, records are also appended to a capped Redis stream so they can be queried. */
  redis?: Redis;
  streamKey?: string;
  maxEntries?: number;
  now?: () => Date;
}

/**
 * Structured audit trail for privileged actions. Every record is emitted as a log line
 * (`audit: true`) for the log pipeline/SIEM and, optionally, persisted in a Redis stream
 * capped at `maxEntries`. Persistence failures are logged but never fail the request.
 */
export const createAuditLog = (options: AuditLogOptions): AuditLog => {
  const log = options.logger.child({ audit: true });
  const now = options.now ?? (() => new Date());
  const streamKey = options.streamKey ?? AUDIT_STREAM_KEY_SUFFIX;
  const maxEntries = options.maxEntries ?? 10_000;
  const persist = options.redis && maxEntries > 0 ? options.redis : undefined;

  return {
    async record(event) {
      const record: AuditRecord = { timestamp: now().toISOString(), ...event };
      const level = record.outcome === 'success' ? 'info' : 'warn';
      log[level]({ event: record }, `audit ${record.action} ${record.outcome}`);
      if (!persist) {
        return;
      }
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
      } catch (error: unknown) {
        log.error({ err: error, action: record.action }, 'Failed to persist audit record');
      }
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
