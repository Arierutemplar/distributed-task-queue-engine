import express from 'express';
import request from 'supertest';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { addMock } = vi.hoisted(() => ({
  addMock: vi.fn(async () => ({ id: 'job-123e4567-e89b-12d3-a456-426614174000' })),
}));

vi.mock('../src/queues/task.queue.js', () => ({
  taskQueue: {
    add: addMock,
  },
}));

import { taskRouter } from '../src/routes/task.routes.js';

const app = express();
app.use(express.json());
app.use('/api/v1/tasks', taskRouter);

describe('POST /api/v1/tasks/process', () => {
  beforeEach(() => {
    addMock.mockClear();
    addMock.mockResolvedValue({
      id: 'job-123e4567-e89b-12d3-a456-426614174000',
    });
  });

  it('returns 400 when the payload fails Zod validation', async () => {
    const response = await request(app)
      .post('/api/v1/tasks/process')
      .send({
        type: 'PROCESS_DOCUMENT',
        documentId: '',
        content: '',
      });

    expect(response.status).toBe(400);
    expect(response.body).toMatchObject({ error: 'Invalid task payload' });
    expect(response.body.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: 'documentId' }),
        expect.objectContaining({ path: 'content' }),
      ]),
    );
    expect(addMock).not.toHaveBeenCalled();
  });

  it('returns 202 and the queue-generated job ID for a valid document task', async () => {
    const payload = {
      type: 'PROCESS_DOCUMENT',
      documentId: 'document-42',
      content: 'A valid document body.',
    };

    const response = await request(app)
      .post('/api/v1/tasks/process')
      .send(payload);

    expect(response.status).toBe(202);
    expect(response.body).toMatchObject({
      taskId: 'job-123e4567-e89b-12d3-a456-426614174000',
      taskType: 'PROCESS_DOCUMENT',
      status: 'accepted',
    });
    expect(response.body.acceptedAt).toEqual(expect.any(String));
    expect(addMock).toHaveBeenCalledWith('PROCESS_DOCUMENT', payload);
  });

  it('returns 202 and the queue-generated job ID for a valid invoice task', async () => {
    const payload = {
      type: 'PROCESS_INVOICE',
      customerId: 'customer-42',
      amount: 125.5,
      currency: 'USD',
      taxId: 'TAX-123',
      items: [
        {
          description: 'Consulting services',
          quantity: 2,
          unitPrice: 62.75,
        },
      ],
    };

    const response = await request(app)
      .post('/api/v1/tasks/process')
      .send(payload);

    expect(response.status).toBe(202);
    expect(response.body).toMatchObject({
      taskId: 'job-123e4567-e89b-12d3-a456-426614174000',
      taskType: 'PROCESS_INVOICE',
      status: 'accepted',
    });
    expect(response.body.acceptedAt).toEqual(expect.any(String));
    expect(addMock).toHaveBeenCalledWith('PROCESS_INVOICE', payload);
  });
});
