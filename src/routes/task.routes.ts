import { Router } from 'express';
import { processTask } from '../controllers/task.controller.js';

export const taskRouter = Router();

taskRouter.post('/process', processTask);
