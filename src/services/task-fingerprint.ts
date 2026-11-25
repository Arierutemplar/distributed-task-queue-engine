import { createHash } from 'node:crypto';
import type { TaskPayload } from '../types/task.types.js';

// Zod emits keys in schema order; omitted and explicit version 1 are equivalent.
export const payloadFingerprint = (payload: TaskPayload): string => {
  const { schemaVersion: _schemaVersion, ...versionOnePayload } = payload;
  return createHash('sha256').update(JSON.stringify(versionOnePayload), 'utf8').digest('hex');
};
