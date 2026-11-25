import { mkdtemp, readFile, writeFile, readdir, unlink, rmdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type * as S3 from '@aws-sdk/client-s3';
import {
  archiveSnapshot,
  backupPrefix,
  createBackupStorage,
  decryptSnapshot,
  downloadSnapshot,
  encryptSnapshot,
  loadBackupSettings,
  type BackupStorage,
} from '../src/operations/backup.js';

const sdk = vi.hoisted(() => ({ send: vi.fn(), destroy: vi.fn() }));
vi.mock('@aws-sdk/client-s3', async (original) => {
  const module = await original<typeof S3>();
  return {
    ...module,
    S3Client: class {
      send = sdk.send;
      destroy = sdk.destroy;
    },
  };
});
const directories: string[] = [];
const fixture = async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dtq-unit-backup-'));
  directories.push(directory);
  const source = join(directory, 'source.rdb');
  await writeFile(source, 'REDIS-test-document');
  const settings = loadBackupSettings({
    BACKUP_S3_BUCKET: 'test-backups',
    BACKUP_ENCRYPTION_KEY: 'ab'.repeat(32),
  });
  const capturedAt = new Date();
  const signal = new AbortController().signal;
  const objects = new Map<string, Buffer>();
  const storage: BackupStorage = {
    put: vi.fn(async (key, file) => {
      objects.set(key, await readFile(file));
      return 'fixture-version';
    }),
    get: vi.fn(async (key) => Readable.from(objects.get(key)!)),
    close: vi.fn(),
  };
  return { directory, source, settings, capturedAt, signal, storage, objects };
};
afterEach(async () => {
  for (const directory of directories.splice(0)) {
    for (const file of await readdir(directory)) await unlink(join(directory, file));
    await rmdir(directory);
  }
});

describe('verified encrypted backups', () => {
  it('rejects different encrypted content even when timestamp and size match', async () => {
    const f = await fixture();
    const other = join(f.directory, 'other.rdb');
    const encrypted = join(f.directory, 'other.enc');
    await writeFile(other, 'REDIS-other-content');
    await encryptSnapshot(other, encrypted, f.settings.BACKUP_ENCRYPTION_KEY, f.capturedAt);
    vi.mocked(f.storage.get).mockResolvedValueOnce(Readable.from(await readFile(encrypted)));
    await expect(
      archiveSnapshot(
        f.settings,
        f.storage,
        f.source,
        f.directory,
        f.capturedAt,
        vi.fn(),
        f.signal,
      ),
    ).rejects.toThrow('differs');
  });
  it('round trips an immutable remote snapshot before reporting success', async () => {
    const f = await fixture();
    const verify = vi.fn(async (file: string) => {
      expect((await readFile(file)).toString()).toBe('REDIS-test-document');
    });
    const report = await archiveSnapshot(
      f.settings,
      f.storage,
      f.source,
      f.directory,
      f.capturedAt,
      verify,
      f.signal,
    );
    expect(report.capturedAt).toBe(f.capturedAt.toISOString());
    expect(report.versionId).toBe('fixture-version');
    expect(f.storage.get).toHaveBeenCalledWith(report.key, f.signal, report.versionId);
    expect(report.key.startsWith(backupPrefix(f.settings))).toBe(true);
    expect(verify).toHaveBeenCalledTimes(2);
    expect(f.objects.get(report.key)?.includes(Buffer.from('REDIS-test-document'))).toBe(false);
  });
  it.each(['tag', 'key', 'header', 'truncated'])(
    'rejects %s corruption without retaining unauthenticated plaintext',
    async (kind) => {
      const f = await fixture();
      const encrypted = join(f.directory, 'encrypted');
      const target = join(f.directory, 'decrypted');
      await encryptSnapshot(f.source, encrypted, f.settings.BACKUP_ENCRYPTION_KEY, f.capturedAt);
      const bytes = await readFile(encrypted);
      if (kind === 'tag') bytes[bytes.length - 1] ^= 1;
      if (kind === 'header') bytes[0] ^= 1;
      await writeFile(encrypted, kind === 'truncated' ? bytes.subarray(0, 10) : bytes);
      await expect(
        decryptSnapshot(
          encrypted,
          target,
          kind === 'key' ? 'cd'.repeat(32) : f.settings.BACKUP_ENCRYPTION_KEY,
        ),
      ).rejects.toThrow();
      expect(await readdir(f.directory)).not.toContain('decrypted');
    },
  );
  it('rejects unsafe prefixes, oversized sources and remote verification failures', async () => {
    const f = await fixture();
    await expect(
      downloadSnapshot(f.settings, f.storage, 'other/key', f.directory, vi.fn(), f.signal),
    ).rejects.toThrow();
    await writeFile(f.source, Buffer.alloc(2048));
    await expect(
      archiveSnapshot(
        { ...f.settings, BACKUP_MAX_BYTES: 1024 },
        f.storage,
        f.source,
        f.directory,
        f.capturedAt,
        vi.fn(),
        f.signal,
      ),
    ).rejects.toThrow('size limit');
    await expect(
      archiveSnapshot(
        f.settings,
        f.storage,
        f.source,
        f.directory,
        f.capturedAt,
        vi.fn().mockRejectedValue(new Error('invalid RDB')),
        f.signal,
      ),
    ).rejects.toThrow('invalid RDB');
  });
  it('propagates storage failures and cancellation rather than claiming success', async () => {
    const f = await fixture();
    vi.mocked(f.storage.put).mockRejectedValue(new Error('archive unavailable'));
    await expect(
      archiveSnapshot(
        f.settings,
        f.storage,
        f.source,
        f.directory,
        f.capturedAt,
        vi.fn(),
        f.signal,
      ),
    ).rejects.toThrow('unavailable');
    const controller = new AbortController();
    controller.abort();
    await expect(
      encryptSnapshot(
        f.source,
        join(f.directory, 'aborted'),
        f.settings.BACKUP_ENCRYPTION_KEY,
        f.capturedAt,
        controller.signal,
      ),
    ).rejects.toThrow();
  });
  it('sets COMPLIANCE retention/checksum and requires a versioned acknowledgement', async () => {
    const f = await fixture();
    const storage = createBackupStorage(f.settings);
    sdk.send.mockResolvedValueOnce({ VersionId: 'v1' });
    await storage.put('key', f.source, 19, 'checksum', f.signal);
    expect(sdk.send.mock.calls[0][0].input).toMatchObject({
      ObjectLockMode: 'COMPLIANCE',
      ChecksumSHA256: 'checksum',
      Bucket: 'test-backups',
    });
    expect(sdk.send.mock.calls[0][0].input.ObjectLockRetainUntilDate.getTime()).toBeGreaterThan(
      Date.now() + 89 * 86400000,
    );
    sdk.send.mockResolvedValueOnce({ VersionId: 'null' });
    await expect(storage.put('key', f.source, 19, 'checksum', f.signal)).rejects.toThrow(
      'versioned',
    );
    sdk.send.mockResolvedValueOnce({ Body: 'not a stream' });
    await expect(storage.get('key', f.signal)).rejects.toThrow('readable');
    const body = Readable.from('data');
    sdk.send.mockResolvedValueOnce({ Body: body });
    expect(await storage.get('key', f.signal)).toBe(body);
    storage.close();
    expect(sdk.destroy).toHaveBeenCalled();
  });
  it.each([
    { BACKUP_S3_ACCESS_KEY_ID: 'unpaired' },
    { BACKUP_S3_ENDPOINT: 'http://external.example.com' },
    { BACKUP_S3_ENDPOINT: 'http://minio:9000', PRODUCTION_HARDENING_ENABLED: 'true' },
    { BACKUP_INTERVAL_MS: '900000' },
  ])('rejects unsafe backup configuration: %j', (override) => {
    expect(() =>
      loadBackupSettings({
        BACKUP_S3_BUCKET: 'test-backups',
        BACKUP_ENCRYPTION_KEY: 'ab'.repeat(32),
        ...override,
      }),
    ).toThrow();
  });
});
