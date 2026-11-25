import { execFile } from 'node:child_process';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { promisify } from 'node:util';
import { afterAll, beforeAll, expect, it } from 'vitest';

const execute = promisify(execFile);
let server: Server;
let base: string;

beforeAll(async () => {
  server = createServer((request, response) => {
    if (request.headers.authorization !== 'Bearer test-token') {
      response.writeHead(401).end();
    } else if (request.url === '/health/ready') {
      response.setHeader('content-type', 'application/json');
      response.end('{"status":"ready"}');
    } else {
      response.end('queue_jobs{queue="tasks",state="waiting"} 0\n');
    }
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) =>
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    }),
  );
});

it('reports external readiness and metrics latency with optional bearer auth', async () => {
  const { stdout } = await execute(process.execPath, ['ops/reliability/external-probe.js', base], {
    env: { ...process.env, EXTERNAL_PROBE_BEARER_TOKEN: 'test-token' },
  });
  const report = JSON.parse(stdout);
  expect(report.ok).toBe(true);
  expect(report.checks).toEqual([
    expect.objectContaining({
      path: '/health/ready',
      status: 200,
      ok: true,
      latencyMs: expect.any(Number),
    }),
    expect.objectContaining({
      path: '/metrics',
      status: 200,
      ok: true,
      latencyMs: expect.any(Number),
    }),
  ]);
  expect(stdout).not.toContain('test-token');
});

it('exits nonzero on rejected checks or insecure non-loopback HTTP', async () => {
  await expect(
    execute(process.execPath, ['ops/reliability/external-probe.js', base], {
      env: { ...process.env, EXTERNAL_PROBE_BEARER_TOKEN: '' },
    }),
  ).rejects.toMatchObject({ code: 1, stdout: expect.stringContaining('"ok":false') });
  await expect(
    execute(process.execPath, ['ops/reliability/external-probe.js', 'http://example.test']),
  ).rejects.toMatchObject({ code: 1, stderr: expect.stringContaining('Use HTTPS') });
});
