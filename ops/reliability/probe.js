// Runs only in the isolated reliability fixture.
import process from 'node:process';
import { setTimeout as sleep } from 'node:timers/promises';
import { Queue, Worker } from 'bullmq';
import { Redis } from 'ioredis';
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
const queue = new Queue(mode === 'unrouted' ? 'tasks' : 'audit-worm-outbox', {
  connection,
  ...(mode === 'unrouted' ? {} : { prefix: `${process.env.SERVICE_NAME}:bull` }),
});
let worker;
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
  } else throw new Error('Unknown probe mode');
} finally {
  await worker?.close(true);
  await queue.close();
  connection.disconnect();
}
