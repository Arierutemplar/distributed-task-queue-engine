import { createHash } from 'node:crypto';
import type { TaskPayload, TaskResult } from '../types/task.types.js';

export const processDocument = (
  payload: Extract<TaskPayload, { type: 'PROCESS_DOCUMENT' }>,
): Extract<TaskResult, { taskType: 'PROCESS_DOCUMENT' }> => {
  const trimmedContent = payload.content.trim();
  return {
    taskType: 'PROCESS_DOCUMENT',
    documentId: payload.documentId,
    characterCount: Array.from(payload.content).length,
    wordCount: trimmedContent.length === 0 ? 0 : trimmedContent.split(/\s+/u).length,
    sha256: createHash('sha256').update(payload.content, 'utf8').digest('hex'),
    completedAt: new Date().toISOString(),
  };
};
