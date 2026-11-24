import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { ExpressAdapter } from '@bull-board/express';
import type { Express } from 'express';
import { deadLetterQueue } from '../queues/dead-letter.queue.js';
import { taskQueue } from '../queues/task.queue.js';

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

createBullBoard({
  queues: [
    new BullMQAdapter(taskQueue),
    new BullMQAdapter(deadLetterQueue, { readOnlyMode: true }),
  ],
  serverAdapter,
});

// getRouter() returns a standalone express() sub-app that does not inherit the parent's settings.
export const boardRouter: Express = serverAdapter.getRouter() as Express;
boardRouter.disable('x-powered-by');
