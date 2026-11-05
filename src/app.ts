import cors from 'cors';
import express, {
  type ErrorRequestHandler,
  type Request,
  type Response,
} from 'express';
import { logger } from './config/redis.js';
import { boardRouter } from './routes/board.routes.js';
import { taskRouter } from './routes/task.routes.js';

export const app = express();

app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use('/api/v1/tasks', taskRouter);
app.use('/admin/queues', boardRouter);

app.use((_request, response) => {
  response.status(404).json({ error: 'Route not found' });
});

const errorHandler: ErrorRequestHandler = (
  error: unknown,
  _request: Request,
  response: Response,
  _next,
) => {
  if (
    error instanceof SyntaxError &&
    'status' in error &&
    error.status === 400
  ) {
    response.status(400).json({ error: 'Invalid JSON request body' });
    return;
  }

  logger.error({ err: error }, 'Unhandled request error');
  response.status(500).json({ error: 'Internal server error' });
};

app.use(errorHandler);
