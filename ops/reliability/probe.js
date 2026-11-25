// Runs only in the isolated reliability fixture.
import process from 'node:process';
import { setTimeout as sleep } from 'node:timers/promises';
import { Queue, QueueEvents, Worker } from 'bullmq';
import { Redis } from 'ioredis';
import { captureRestoreSamples } from '../../dist/src/operations/restore-samples.js';
import { processDocument } from '../../dist/src/services/document.service.js';
const connection = new Redis({
  host: 'redis',
  password: process.env.REDIS_PASSWORD,
  maxRetriesPerRequest: null,
  ...(process.env.REDIS_SENTINEL_HOSTS
    ? {
        sentinels: process.env.REDIS_SENTINEL_HOSTS.split(',').map((value) => ({
          host: value.split(':')[0],
          port: Number(value.split(':')[1]),
        })),
        name: process.env.REDIS_SENTINEL_MASTER_NAME,
        sentinelPassword: process.env.REDIS_SENTINEL_PASSWORD || undefined,
      }
    : {}),
});
connection.on('error', (error) => {
  process.stderr.write(`Probe Redis error: ${error.name}\n`);
});
const mode = process.argv[2];
const taskMode = mode === 'unrouted' || mode === 'seed-sample';
const queue = new Queue(taskMode ? 'tasks' : 'audit-worm-outbox', {
  connection,
  ...(taskMode ? {} : { prefix: `${process.env.SERVICE_NAME}:bull` }),
});
let worker;
let events;
let audit;
try {
  if (mode === 'pause') await queue.pause();
  else if (mode === 'resume') await queue.resume();
  else if (mode === 'counts')
    process.stdout.write(
      `${JSON.stringify(await queue.getJobCounts('waiting', 'active', 'delayed', 'failed', 'paused'))}\n`,
    );
  else if (mode === 'unrouted') {
    worker = new Worker(
      'tasks',
      async () => {
        throw new Error('Lost callback drill');
      },
      { connection },
    );
    const job = await queue.add(
      'PROCESS_DOCUMENT',
      { type: 'PROCESS_DOCUMENT', documentId: 'unrouted', content: 'failure drill' },
      { attempts: 1, removeOnFail: false },
    );
    const deadline = Date.now() + 15000;
    while ((await job.getState()) !== 'failed') {
      if (Date.now() > deadline) throw new Error('Unrouted failure not persisted');
      await sleep(50);
    }
    process.stdout.write(`${JSON.stringify({ kind: 'unrouted-report', taskId: job.id })}\n`);
  } else if (mode === 'seed-sample') {
    const failedId = process.argv[3];
    if (!failedId) throw new Error('Missing previously reconciled task id');
    events = new QueueEvents('tasks', { connection });
    worker = new Worker(
      'tasks',
      async (job) => {
        if (job.data.documentId === 'sample-failed') throw new Error('Representative failure');
        return processDocument(job.data);
      },
      { connection },
    );
    await Promise.all([events.waitUntilReady(), worker.waitUntilReady()]);
    const data = (documentId) => ({
      type: 'PROCESS_DOCUMENT',
      documentId,
      content: `representative content for ${documentId}`,
    });
    const completed = await queue.add('PROCESS_DOCUMENT', data('sample-completed'));
    await completed.waitUntilFinished(events, 15_000);
    const failed = await queue.add('PROCESS_DOCUMENT', data('sample-failed'), {
      attempts: 1,
      removeOnFail: false,
    });
    try {
      await failed.waitUntilFinished(events, 15_000);
      throw new Error('Representative failure unexpectedly completed');
    } catch (error) {
      if (error.message !== 'Representative failure') throw error;
    }
    await worker.close();
    worker = undefined;
    const waiting = await queue.add('PROCESS_DOCUMENT', data('sample-waiting'));
    const delayed = await queue.add('PROCESS_DOCUMENT', data('sample-delayed'), {
      delay: 86_400_000,
    });
    audit = new Queue('audit-worm-outbox', {
      connection,
      prefix: `${process.env.SERVICE_NAME}:bull`,
    });
    const [outbox] = await audit.getJobs(['paused'], 0, 0);
    const [entry] = await connection.xrevrange(
      `${process.env.SERVICE_NAME}:audit:admin`,
      '+',
      '-',
      'COUNT',
      1,
    );
    if (!outbox?.id || !entry?.[0]) throw new Error('Missing pending outbox or audit entry');
    const manifest = await captureRestoreSamples(connection, process.env.SERVICE_NAME, {
      jobs: [
        ...[completed, failed, waiting, delayed].map((job) => ({ queue: 'tasks', id: job.id })),
        { queue: 'tasks-dead-letter', id: `dlq-${failedId}` },
        { queue: 'audit-worm-outbox', id: outbox.id },
      ],
      auditId: entry[0],
    });
    const expected = ['completed', 'failed', 'waiting', 'delayed'];
    if (expected.some((state, index) => manifest.jobs[index].state !== state))
      throw new Error(`Unexpected representative task states: ${JSON.stringify(manifest.jobs)}`);
    process.stdout.write(`${JSON.stringify({ kind: 'representative-sample', manifest })}\n`);
  } else throw new Error('Unknown probe mode');
} finally {
  await worker?.close(true);
  await events?.close();
  await audit?.close();
  await queue.close();
  connection.disconnect();
}
