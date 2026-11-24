import { Router } from 'express';
import { getTask, processTask } from '../controllers/task.controller.js';

export const taskRouter = Router();

taskRouter.post('/process', processTask);
taskRouter.get('/:taskId', getTask);
