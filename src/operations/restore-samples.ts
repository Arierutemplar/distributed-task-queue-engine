import { createHash } from 'node:crypto';
import { Queue } from 'bullmq';
import type { Redis } from 'ioredis';
import { z } from 'zod';

const referenceSchema = z.object({
  queue: z.enum(['tasks', 'tasks-dead-letter', 'audit-worm-outbox']),
  id: z.string().min(1).max(200),
});
const sampleSchema = referenceSchema.extend({
  state: z.string().min(1),
  digest: z.string().regex(/^[a-f0-9]{64}$/u),
});
export const restoreManifestSchema = z.object({
  jobs: z.array(sampleSchema).min(1).max(20),
  audit: z.object({
    id: z.string().regex(/^\d+-\d+$/u),
    digest: z.string().regex(/^[a-f0-9]{64}$/u),
  }),
});
export interface RestoreReferences {
  jobs: z.infer<typeof referenceSchema>[];
  auditId: string;
}

const digest = (value: unknown): string =>
  createHash('sha256').update(JSON.stringify(value)).digest('hex');

export async function captureRestoreSamples(
  client: Redis,
  service: string,
  references: RestoreReferences,
): Promise<z.infer<typeof restoreManifestSchema>> {
  const queues = {
    tasks: new Queue('tasks', { connection: client }),
    'tasks-dead-letter': new Queue('tasks-dead-letter', { connection: client }),
    'audit-worm-outbox': new Queue('audit-worm-outbox', {
      connection: client,
      prefix: `${service}:bull`,
    }),
  };
  try {
    const jobs = await Promise.all(
      references.jobs.map(async (reference) => {
        const job = await queues[reference.queue].getJob(reference.id);
        if (!job) throw new Error(`Missing sampled job ${reference.queue}/${reference.id}`);
        return {
          ...reference,
          state: await job.getState(),
          digest: digest([job.data, job.returnvalue, job.failedReason]),
        };
      }),
    );
    const entries = await client.xrange(
      `${service}:audit:admin`,
      references.auditId,
      references.auditId,
    );
    if (entries.length !== 1) throw new Error('Missing sampled audit stream entry');
    return { jobs, audit: { id: references.auditId, digest: digest(entries[0]) } };
  } finally {
    await Promise.all(Object.values(queues).map((queue) => queue.close()));
  }
}

export async function verifyRestoreSamples(
  client: Redis,
  service: string,
  manifest: z.infer<typeof restoreManifestSchema>,
): Promise<void> {
  const observed = await captureRestoreSamples(client, service, {
    jobs: manifest.jobs.map(({ queue, id }) => ({ queue, id })),
    auditId: manifest.audit.id,
  });
  if (JSON.stringify(observed) !== JSON.stringify(manifest))
    throw new Error('Restored sample state or content does not match the manifest');
}
