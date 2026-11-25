import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { appendFile, open, stat, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { z } from 'zod';

const optional = z.preprocess(
  (value) => (typeof value === 'string' && value.trim() === '' ? undefined : value),
  z.string().optional(),
);
const settingsSchema = z
  .object({
    SERVICE_NAME: z.string().min(1).default('distributed-task-queue'),
    BACKUP_S3_ENDPOINT: optional.pipe(z.url().optional()),
    BACKUP_S3_BUCKET: z.string().regex(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u),
    BACKUP_S3_REGION: z.string().min(1).default('us-east-1'),
    BACKUP_S3_ACCESS_KEY_ID: optional,
    BACKUP_S3_SECRET_ACCESS_KEY: optional,
    BACKUP_ENCRYPTION_KEY: z.string().regex(/^[a-fA-F0-9]{64}$/u),
    BACKUP_KEY_ID: z
      .string()
      .regex(/^[A-Za-z0-9_-]+$/u)
      .default('v1'),
    BACKUP_RETENTION_DAYS: z.coerce.number().int().min(1).max(36_500).default(90),
    BACKUP_INTERVAL_MS: z.coerce.number().int().min(1_000).max(900_000).default(300_000),
    BACKUP_TIMEOUT_MS: z.coerce.number().int().min(1_000).max(600_000).default(240_000),
    BACKUP_MAX_BYTES: z.coerce
      .number()
      .int()
      .min(1_024)
      .max(100 * 1_024 ** 3)
      .default(1_024 ** 3),
    BACKUP_RPO_SECONDS: z.coerce.number().int().min(1).max(86_400).default(900),
    BACKUP_RTO_SECONDS: z.coerce.number().int().min(1).max(86_400).default(900),
    BACKUP_PORT: z.coerce.number().int().min(1).max(65_535).default(3_100),
    PRODUCTION_HARDENING_ENABLED: z.enum(['true', 'false']).default('false'),
  })
  .superRefine((settings, context) => {
    if (
      Boolean(settings.BACKUP_S3_ACCESS_KEY_ID) !== Boolean(settings.BACKUP_S3_SECRET_ACCESS_KEY)
    ) {
      context.addIssue({
        code: 'custom',
        path: ['BACKUP_S3_ACCESS_KEY_ID'],
        message: 'supply both backup S3 credentials or use the AWS credential chain',
      });
    }
    if (settings.BACKUP_S3_ENDPOINT) {
      const url = new URL(settings.BACKUP_S3_ENDPOINT);
      const local = ['localhost', '127.0.0.1', '[::1]', 'minio', 'fixture-s3'].includes(
        url.hostname,
      );
      if (
        url.username ||
        url.password ||
        url.search ||
        url.hash ||
        (url.protocol !== 'https:' &&
          !(local && url.protocol === 'http:' && settings.PRODUCTION_HARDENING_ENABLED === 'false'))
      ) {
        context.addIssue({
          code: 'custom',
          path: ['BACKUP_S3_ENDPOINT'],
          message: 'requires HTTPS without credentials/query/fragment (local tests permit HTTP)',
        });
      }
    }
    if (
      settings.BACKUP_INTERVAL_MS + settings.BACKUP_TIMEOUT_MS >
      settings.BACKUP_RPO_SECONDS * 1_000
    ) {
      context.addIssue({
        code: 'custom',
        path: ['BACKUP_INTERVAL_MS'],
        message: 'interval plus execution budget must fit the RPO objective',
      });
    }
  });
export type BackupSettings = z.infer<typeof settingsSchema>;
export const loadBackupSettings = (source: NodeJS.ProcessEnv): BackupSettings =>
  settingsSchema.parse(source);
export class BackupError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BackupError';
  }
}
export const backupPrefix = (settings: BackupSettings): string =>
  `${encodeURIComponent(settings.SERVICE_NAME)}/backups/${settings.BACKUP_KEY_ID}/`;

export interface BackupStorage {
  put: (
    key: string,
    file: string,
    bytes: number,
    checksum: string,
    signal: AbortSignal,
  ) => Promise<string>;
  get: (key: string, signal: AbortSignal, versionId?: string) => Promise<Readable>;
  close: () => void;
}
export const createBackupStorage = (settings: BackupSettings): BackupStorage => {
  const client = new S3Client({
    endpoint: settings.BACKUP_S3_ENDPOINT,
    region: settings.BACKUP_S3_REGION,
    forcePathStyle: Boolean(settings.BACKUP_S3_ENDPOINT),
    maxAttempts: 1,
    credentials:
      settings.BACKUP_S3_ACCESS_KEY_ID && settings.BACKUP_S3_SECRET_ACCESS_KEY
        ? {
            accessKeyId: settings.BACKUP_S3_ACCESS_KEY_ID,
            secretAccessKey: settings.BACKUP_S3_SECRET_ACCESS_KEY,
          }
        : undefined,
  });
  return {
    async put(key, file, bytes, checksum, signal) {
      const body = createReadStream(file);
      try {
        const response = await client.send(
          new PutObjectCommand({
            Bucket: settings.BACKUP_S3_BUCKET,
            Key: key,
            Body: body,
            ContentLength: bytes,
            ContentType: 'application/octet-stream',
            ChecksumSHA256: checksum,
            ObjectLockMode: 'COMPLIANCE',
            ObjectLockRetainUntilDate: new Date(
              Date.now() + settings.BACKUP_RETENTION_DAYS * 86_400_000,
            ),
          }),
          { abortSignal: signal },
        );
        if (!response.VersionId || response.VersionId === 'null')
          throw new BackupError('Backup storage did not acknowledge a versioned object');
        return response.VersionId;
      } finally {
        body.destroy();
      }
    },
    async get(key, signal, versionId) {
      const response = await client.send(
        new GetObjectCommand({
          Bucket: settings.BACKUP_S3_BUCKET,
          Key: key,
          VersionId: versionId,
        }),
        { abortSignal: signal },
      );
      if (!(response.Body instanceof Readable))
        throw new BackupError('Backup object is not a readable stream');
      return response.Body;
    },
    close: () => {
      client.destroy();
    },
  };
};

const magic = Buffer.from('DTQRDB01');
const headerLength = 28;
const overhead = headerLength + 16;
export const encryptSnapshot = async (
  source: string,
  destination: string,
  key: string,
  capturedAt: Date,
  signal?: AbortSignal,
): Promise<void> => {
  const header = Buffer.alloc(headerLength);
  magic.copy(header);
  const nonce = randomBytes(12);
  nonce.copy(header, 8);
  header.writeBigUInt64BE(BigInt(capturedAt.getTime()), 20);
  const cipher = createCipheriv('aes-256-gcm', Buffer.from(key, 'hex'), nonce);
  cipher.setAAD(header);
  const output = createWriteStream(destination, { flags: 'wx', mode: 0o600 });
  output.write(header);
  await pipeline(createReadStream(source), cipher, output, { signal });
  await appendFile(destination, cipher.getAuthTag());
};

export const decryptSnapshot = async (
  source: string,
  destination: string,
  key: string,
  signal?: AbortSignal,
): Promise<Date> => {
  const file = await open(source, 'r');
  const header = Buffer.alloc(headerLength);
  const tag = Buffer.alloc(16);
  let bytes: number;
  try {
    bytes = (await file.stat()).size;
    if (bytes < overhead) throw new BackupError('Backup envelope is truncated');
    await file.read(header, 0, headerLength, 0);
    await file.read(tag, 0, 16, bytes - 16);
  } finally {
    await file.close();
  }
  if (!header.subarray(0, 8).equals(magic)) throw new BackupError('Unsupported backup format');
  const millis = Number(header.readBigUInt64BE(20));
  if (!Number.isSafeInteger(millis) || millis > Date.now() + 60_000)
    throw new BackupError('Invalid or future backup capture time');
  const decipher = createDecipheriv('aes-256-gcm', Buffer.from(key, 'hex'), header.subarray(8, 20));
  decipher.setAAD(header);
  decipher.setAuthTag(tag);
  const state = { created: false };
  const output = createWriteStream(destination, { flags: 'wx', mode: 0o600 });
  output.once('open', () => {
    state.created = true;
  });
  try {
    await pipeline(
      createReadStream(source, { start: headerLength, end: bytes - 17 }),
      decipher,
      output,
      { signal },
    );
  } catch {
    if (state.created) await unlink(destination);
    throw new BackupError('Backup authentication or output I/O failed');
  }
  return new Date(millis);
};

export const downloadSnapshot = async (
  settings: BackupSettings,
  storage: BackupStorage,
  key: string,
  directory: string,
  verifyRdb: (file: string) => Promise<void>,
  signal: AbortSignal,
  versionId?: string,
): Promise<{ file: string; capturedAt: Date }> => {
  if (
    !key.startsWith(backupPrefix(settings)) ||
    !/^[A-Za-z0-9%._/-]+$/u.test(key) ||
    key.split('/').some((part) => part === '..')
  )
    throw new BackupError('Object key is outside the configured backup prefix');
  const encrypted = join(directory, 'download.enc');
  let bytes = 0;
  const limit = new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      bytes += chunk.length;
      callback(
        bytes > settings.BACKUP_MAX_BYTES + overhead
          ? new BackupError('Backup object exceeds the configured size limit')
          : null,
        chunk,
      );
    },
  });
  await pipeline(
    await storage.get(key, signal, versionId),
    limit,
    createWriteStream(encrypted, { flags: 'wx', mode: 0o600 }),
    { signal },
  );
  const file = join(directory, 'restore.rdb');
  const capturedAt = await decryptSnapshot(encrypted, file, settings.BACKUP_ENCRYPTION_KEY, signal);
  await verifyRdb(file);
  return { file, capturedAt };
};

export const archiveSnapshot = async (
  settings: BackupSettings,
  storage: BackupStorage,
  source: string,
  directory: string,
  capturedAt: Date,
  verifyRdb: (file: string) => Promise<void>,
  signal: AbortSignal,
): Promise<{ key: string; versionId: string; capturedAt: string; bytes: number }> => {
  const bytes = (await stat(source)).size;
  if (bytes > settings.BACKUP_MAX_BYTES)
    throw new BackupError('Snapshot exceeds the configured size limit');
  await verifyRdb(source);
  const sourceChecksum = await snapshotChecksum(source, signal);
  const encrypted = join(directory, 'upload.enc');
  await encryptSnapshot(source, encrypted, settings.BACKUP_ENCRYPTION_KEY, capturedAt, signal);
  const checksum = await snapshotChecksum(encrypted, signal);
  const key = `${backupPrefix(settings)}${capturedAt.toISOString().slice(0, 10)}/${randomUUID()}.rdb.enc`;
  const versionId = await storage.put(
    key,
    encrypted,
    (await stat(encrypted)).size,
    checksum,
    signal,
  );
  const verified = await downloadSnapshot(
    settings,
    storage,
    key,
    directory,
    verifyRdb,
    signal,
    versionId,
  );
  if (
    verified.capturedAt.getTime() !== capturedAt.getTime() ||
    (await stat(verified.file)).size !== bytes ||
    (await snapshotChecksum(verified.file, signal)) !== sourceChecksum
  )
    throw new BackupError('Remote snapshot verification differs from the captured snapshot');
  return { key, versionId, capturedAt: capturedAt.toISOString(), bytes };
};

const snapshotChecksum = async (file: string, signal: AbortSignal): Promise<string> => {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(file, { signal })) {
    const bytes: unknown = chunk;
    if (!Buffer.isBuffer(bytes)) throw new BackupError('Unexpected snapshot stream encoding');
    hash.update(bytes);
  }
  return hash.digest('base64');
};
