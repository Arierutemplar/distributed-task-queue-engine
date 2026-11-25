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

export const createAuditWormSink = (
  settings: AppConfig,
  logger: Logger,
  options: AuditWormOptions = {},
): AuditSink | undefined => {
  if (!settings.AUDIT_WORM_ENABLED) return undefined;
  const log = logger.child({ auditWorm: true });
  if (!settings.AUDIT_WORM_ACCESS_KEY_ID || !settings.AUDIT_WORM_SECRET_ACCESS_KEY) {
    log.warn('WORM archive is enabled but credentials are incomplete; using Redis/logs only');
    return undefined;
  }
  const client: AuditS3Client =
    options.client ??
    new S3Client({
      endpoint: settings.AUDIT_WORM_ENDPOINT,
      region: settings.AUDIT_WORM_REGION,
      forcePathStyle: settings.AUDIT_WORM_FORCE_PATH_STYLE,
      credentials: {
        accessKeyId: settings.AUDIT_WORM_ACCESS_KEY_ID,
        secretAccessKey: settings.AUDIT_WORM_SECRET_ACCESS_KEY,
      },
      maxAttempts: 1,
    });
  const now = options.now ?? (() => new Date());
  const pending = new Set<Promise<void>>();
  let closed = false;

  const archive = async (record: AuditRecord): Promise<void> => {
    const bucket = settings.AUDIT_WORM_BUCKET;
    const key = `${encodeURIComponent(settings.SERVICE_NAME)}/${record.timestamp.slice(0, 10)}/${randomUUID()}.json`;
    try {
      const body = JSON.stringify(record);
      const retainUntil = new Date(
        now().getTime() + settings.AUDIT_WORM_RETENTION_DAYS * 86_400_000,
      );
      const result = await withTimeout(
        (signal) =>
          client.send(
            new PutObjectCommand({
              Bucket: bucket,
              Key: key,
              Body: body,
              ContentType: 'application/json',
              ChecksumAlgorithm: 'SHA256',
              ChecksumSHA256: createHash('sha256').update(body).digest('base64'),
              ObjectLockMode: settings.AUDIT_WORM_LOCK_MODE,
              ObjectLockRetainUntilDate: retainUntil,
            }),
            { abortSignal: signal },
          ),
        settings.AUDIT_WORM_TIMEOUT_MS,
      );
      if (!result.VersionId || result.VersionId === 'null') {
        throw new Error('Archive destination did not return a versioned object');
      }
      log.info(
        {
          bucket,
          key,
          versionId: result.VersionId,
          lockMode: settings.AUDIT_WORM_LOCK_MODE,
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
    }
  };

  return {
    write(record) {
      if (closed) {
        log.error('WORM archive is closed; record remains in Redis/logs only');
        return Promise.resolve();
      }
      const operation = archive(record);
      pending.add(operation);
      return operation.finally(() => {
        pending.delete(operation);
      });
    },
    async close() {
      closed = true;
      await Promise.all(pending);
      client.destroy();
    },
  };
};
