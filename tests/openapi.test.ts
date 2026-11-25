import SwaggerParser from '@apidevtools/swagger-parser';
import { describe, expect, it } from 'vitest';

describe('OpenAPI contract', () => {
  it('is a valid specification with the application routes', async () => {
    const api = await SwaggerParser.validate('openapi.yaml');
    expect(Object.keys(api.paths ?? {}).sort()).toEqual(
      [
        '/health/live',
        '/health/ready',
        '/metrics',
        '/api/v1/tasks/process',
        '/api/v1/tasks/{taskId}',
        '/admin/queues',
        '/admin/api/dead-letter',
        '/admin/api/dead-letter/{taskId}/replay',
        '/admin/api/dead-letter/{taskId}',
        '/admin/api/audit',
        '/admin/login',
        '/admin/callback',
        '/admin/logout',
      ].sort(),
    );
  });
});
