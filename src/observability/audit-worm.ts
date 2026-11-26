import { createHash, randomUUID } from 'node:crypto';
import { PutObjectCommand, S3Client, type PutObjectCommandOutput } from '@aws-sdk/client-s3';
import type { Logger } from 'pino';
import type { AppConfig } from '../config/env.js';
import { withTimeout } from '../resilience/timeout.js';
import type { AuditRecord, AuditSink } from './audit.js';

export interface AuditS3Client {
  send: (
    command: PutObjectCommand,
    options: { abortSignal: AbortSignal },
  ) => Promise<PutObjectCommandOutput>;
  destroy: () => void;
}

export interface AuditWormOptions {
  client?: AuditS3Client;
  now?: () => Date;
}

export class AuditArchiveError extends Error {
  constructor(readonly errorType: string) {
    super(`WORM delivery failed: ${errorType}`);
    this.name = 'AuditArchiveError';
  }
}

export interface AuditWormSink extends AuditSink {
  deliver: (record: AuditRecord) => Promise<void>;
  updateCredentials: (accessKeyId: string, secretAccessKey: string) => void;
  updateSettings: (settings: AppConfig) => void;
}

export const createAuditWormSink = (
  settings: AppConfig,
  logger: Logger,
  options: AuditWormOptions = {},
): AuditWormSink | undefined => {
  if (!settings.AUDIT_WORM_ENABLED) return undefined;
  const log = logger.child({ auditWorm: true });
  if (!settings.AUDIT_WORM_ACCESS_KEY_ID || !settings.AUDIT_WORM_SECRET_ACCESS_KEY) {
    log.warn('WORM archive is enabled but credentials are incomplete; using Redis/logs only');
    return undefined;
  }
  let activeSettings: AppConfig = { ...settings };
  if (!activeSettings.AUDIT_WORM_ACCESS_KEY_ID || !activeSettings.AUDIT_WORM_SECRET_ACCESS_KEY)
    throw new Error('WORM credentials must be complete');
  const createClient = (
    candidate: AppConfig,
    accessKeyId: string,
    secretAccessKey: string,
  ): AuditS3Client =>
    new S3Client({
      endpoint: candidate.AUDIT_WORM_ENDPOINT,
      region: candidate.AUDIT_WORM_REGION,
      forcePathStyle: candidate.AUDIT_WORM_FORCE_PATH_STYLE,
      credentials: { accessKeyId, secretAccessKey },
      maxAttempts: 1,
    });
  let client: AuditS3Client =
    options.client ??
    createClient(
      activeSettings,
      activeSettings.AUDIT_WORM_ACCESS_KEY_ID,
      activeSettings.AUDIT_WORM_SECRET_ACCESS_KEY,
    );
  const now = options.now ?? (() => new Date());
  const pending = new Set<Promise<void>>();
  const clients = new Map<AuditS3Client, Set<Promise<void>>>([[client, new Set()]]);
  let closed = false;

  const archive = async (
    record: AuditRecord,
    activeClient: AuditS3Client,
    deliverySettings: AppConfig,
  ): Promise<void> => {
    const bucket = deliverySettings.AUDIT_WORM_BUCKET;
    const key = `${encodeURIComponent(deliverySettings.SERVICE_NAME)}/${record.timestamp.slice(0, 10)}/${record.archiveId ?? randomUUID()}.json`;
    try {
      const body = JSON.stringify(record);
      const retainUntil = new Date(
        now().getTime() + deliverySettings.AUDIT_WORM_RETENTION_DAYS * 86_400_000,
      );
      const result = await withTimeout(
        (signal) =>
          activeClient.send(
            new PutObjectCommand({
              Bucket: bucket,
              Key: key,
              Body: body,
              ContentType: 'application/json',
              ChecksumAlgorithm: 'SHA256',
              ChecksumSHA256: createHash('sha256').update(body).digest('base64'),
              ObjectLockMode: deliverySettings.AUDIT_WORM_LOCK_MODE,
              ObjectLockRetainUntilDate: retainUntil,
            }),
            { abortSignal: signal },
          ),
        deliverySettings.AUDIT_WORM_TIMEOUT_MS,
      );
      if (!result.VersionId || result.VersionId === 'null') {
        throw new Error('Archive destination did not return a versioned object');
      }
      log.info(
        {
          bucket,
          key,
          versionId: result.VersionId,
          lockMode: deliverySettings.AUDIT_WORM_LOCK_MODE,
          retainUntil: retainUntil.toISOString(),
        },
        'WORM audit write succeeded',
      );
    } catch (error: unknown) {
      // SDK errors can contain request headers, URLs or credentials; never log the raw error.
      const name = error instanceof Error ? error.name : '';
      const errorType = [
        'AccessDenied',
        'NoSuchBucket',
        'InvalidRequest',
        'InvalidArgument',
        'InvalidAccessKeyId',
        'SignatureDoesNotMatch',
        'AbortError',
        'TimeoutError',
        'TaskTimeoutError',
        'ServiceUnavailable',
        'SlowDown',
        'InternalError',
      ].includes(name)
        ? name
        : 'ArchiveError';
      log.error({ bucket, key, errorType }, 'WORM audit write failed; Redis/log audit is retained');
      throw new AuditArchiveError(errorType);
    }
  };

  const deliver = (record: AuditRecord): Promise<void> => {
    if (closed) {
      log.error('WORM archive is closed; record remains in Redis/logs only');
      return Promise.reject(new AuditArchiveError('ArchiveClosed'));
    }
    const activeClient = client;
    const deliverySettings = activeSettings;
    const operations = clients.get(activeClient);
    if (!operations) throw new Error('WORM client is unavailable');
    const operation = archive(record, activeClient, deliverySettings);
    pending.add(operation);
    operations.add(operation);
    return operation.finally(() => {
      pending.delete(operation);
      operations.delete(operation);
      if (activeClient !== client && operations.size === 0) {
        activeClient.destroy();
        clients.delete(activeClient);
      }
    });
  };

  return {
    deliver,
    updateCredentials(accessKeyId, secretAccessKey) {
      activeSettings = {
        ...activeSettings,
        AUDIT_WORM_ACCESS_KEY_ID: accessKeyId,
        AUDIT_WORM_SECRET_ACCESS_KEY: secretAccessKey,
      };
      const previous = client;
      client = createClient(activeSettings, accessKeyId, secretAccessKey);
      clients.set(client, new Set());
      const operations = clients.get(previous);
      if (operations?.size === 0) {
        previous.destroy();
        clients.delete(previous);
      }
    },
    updateSettings(candidate) {
      activeSettings = { ...candidate };
      const previous = client;
      if (!activeSettings.AUDIT_WORM_ACCESS_KEY_ID || !activeSettings.AUDIT_WORM_SECRET_ACCESS_KEY)
        throw new Error('Reloaded WORM credentials must be complete');
      client = createClient(
        activeSettings,
        activeSettings.AUDIT_WORM_ACCESS_KEY_ID,
        activeSettings.AUDIT_WORM_SECRET_ACCESS_KEY,
      );
      clients.set(client, new Set());
      const operations = clients.get(previous);
      if (operations?.size === 0) {
        previous.destroy();
        clients.delete(previous);
      }
    },
    async write(record) {
      try {
        await deliver(record);
      } catch (error: unknown) {
        if (!(error instanceof AuditArchiveError)) throw error;
        // Direct mode keeps its historical best-effort API behavior.
      }
    },
    async close() {
      closed = true;
      await Promise.allSettled(pending);
      for (const activeClient of clients.keys()) activeClient.destroy();
      clients.clear();
    },
  };
};
