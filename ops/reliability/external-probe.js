import { performance } from 'node:perf_hooks';
import process from 'node:process';
import { URL, pathToFileURL } from 'node:url';

const { fetch, AbortSignal } = globalThis;

export async function probe(baseUrl, options = {}) {
  const url = new URL(baseUrl);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    (url.protocol !== 'https:' &&
      !(url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)))
  ) {
    throw new Error(
      'Use HTTPS without embedded credentials, query or fragment (HTTP only on loopback)',
    );
  }
  const timeoutMs = options.timeoutMs ?? 5_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60_000)
    throw new Error('Timeout must be an integer between 1 and 60000 ms');
  const headers = options.token ? { Authorization: `Bearer ${options.token}` } : {};
  const checks = [];
  for (const path of ['/health/ready', '/metrics']) {
    const start = performance.now();
    try {
      const endpoint = new URL(`${url.pathname.replace(/\/$/u, '')}${path}`, url);
      const response = await fetch(endpoint, {
        headers,
        redirect: 'error',
        signal: AbortSignal.timeout(timeoutMs),
      });
      const body = await response.text();
      const valid =
        path === '/health/ready'
          ? response.ok && JSON.parse(body).status === 'ready'
          : response.ok && body.includes('queue_jobs');
      checks.push({
        path,
        status: response.status,
        latencyMs: Math.round((performance.now() - start) * 100) / 100,
        ok: valid,
      });
    } catch (error) {
      checks.push({
        path,
        latencyMs: Math.round((performance.now() - start) * 100) / 100,
        ok: false,
        error: error instanceof Error ? error.name : 'Error',
      });
    }
  }
  return { kind: 'external-probe', ok: checks.every((check) => check.ok), checks };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    if (process.argv.length !== 3)
      throw new Error('Usage: node ops/reliability/external-probe.js BASE_URL');
    const report = await probe(process.argv[2], {
      token: process.env.EXTERNAL_PROBE_BEARER_TOKEN,
      timeoutMs: process.env.EXTERNAL_PROBE_TIMEOUT_MS
        ? Number(process.env.EXTERNAL_PROBE_TIMEOUT_MS)
        : undefined,
    });
    process.stdout.write(`${JSON.stringify(report)}\n`);
    if (!report.ok) process.exitCode = 1;
  } catch (error) {
    process.stderr.write(`External probe failed: ${error.message}\n`);
    process.exitCode = 1;
  }
}
