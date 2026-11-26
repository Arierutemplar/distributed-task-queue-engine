import process from 'node:process';
import { performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { createHash, randomUUID } from 'node:crypto';
import { z } from 'zod';
const { fetch, AbortSignal } = globalThis;
const settings = z
  .object({
    LOAD_URL: z.url().default('http://app:3000'),
    LOAD_RATE: z.coerce.number().int().min(1).max(100).default(20),
    LOAD_SECONDS: z.coerce.number().int().min(5).max(300).default(30),
    LOAD_MAX_INFLIGHT: z.coerce.number().int().min(1).max(512).default(128),
    LOAD_API_KEY: z.string().min(1),
  })
  .parse(process.env);
const headers = { 'Content-Type': 'application/json', 'X-API-Key': settings.LOAD_API_KEY };
const acceptance = [];
const completion = [];
const outstanding = new Set();
const errors = [];
const statusCounts = new Map();
let verifiedTaskId;
const content = 'reproducible document '.repeat(50);
const sha256 = createHash('sha256').update(content).digest('hex');
const run = async () => {
  const started = performance.now();
  const response = await fetch(`${settings.LOAD_URL}/api/v1/tasks/process`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ type: 'PROCESS_DOCUMENT', documentId: randomUUID(), content }),
    signal: AbortSignal.timeout(15000),
  });
  statusCounts.set(response.status, (statusCounts.get(response.status) ?? 0) + 1);
  if (response.status !== 202) throw new Error(`Acceptance status ${response.status}`);
  acceptance.push(performance.now() - started);
  const { taskId } = await response.json();
  for (;;) {
    const status = await fetch(`${settings.LOAD_URL}/api/v1/tasks/${taskId}`, {
      headers,
      signal: AbortSignal.timeout(15000),
    });
    statusCounts.set(status.status, (statusCounts.get(status.status) ?? 0) + 1);
    if (!status.ok) throw new Error(`Status response ${status.status}`);
    const task = await status.json();
    // State and result are separate Redis reads; completion can become visible between them.
    if (task.state === 'completed' && task.result !== null) {
      if (task.result.sha256 !== sha256) throw new Error('Document result mismatch');
      completion.push(performance.now() - started);
      verifiedTaskId = taskId;
      return;
    }
    if (task.state === 'failed' || performance.now() - started > 15000)
      throw new Error('Task failed or timed out');
    await sleep(50);
  }
};
const total = settings.LOAD_RATE * settings.LOAD_SECONDS;
const started = performance.now();
for (let index = 0; index < total; index += 1) {
  await sleep(Math.max(0, started + (index * 1000) / settings.LOAD_RATE - performance.now()));
  if (outstanding.size >= settings.LOAD_MAX_INFLIGHT) {
    errors.push('Driver concurrency exhausted');
    break;
  }
  const operation = run()
    .catch((error) => {
      errors.push(error.message);
    })
    .finally(() => outstanding.delete(operation));
  outstanding.add(operation);
}
await Promise.all(outstanding);
const p95 = (values) =>
  values.length ? values.sort((a, b) => a - b)[Math.ceil(values.length * 0.95) - 1] : null;
const percentile = (values, ratio) =>
  values.length
    ? [...values].sort((a, b) => a - b)[Math.max(0, Math.ceil(values.length * ratio) - 1)]
    : null;
const elapsedSeconds = (performance.now() - started) / 1000;
const report = {
  kind: 'load-report',
  workload: 'PROCESS_DOCUMENT, 1100 ASCII bytes, open-loop arrival',
  ratePerSecond: settings.LOAD_RATE,
  durationSeconds: settings.LOAD_SECONDS,
  maxInflight: settings.LOAD_MAX_INFLIGHT,
  requested: total,
  completed: completion.length,
  acceptanceP95Ms: p95(acceptance),
  documentP95Ms: p95(completion),
  acceptanceP50Ms: percentile(acceptance, 0.5),
  acceptanceP99Ms: percentile(acceptance, 0.99),
  documentP50Ms: percentile(completion, 0.5),
  documentP99Ms: percentile(completion, 0.99),
  completedPerSecond:
    elapsedSeconds > 0 ? Number((completion.length / elapsedSeconds).toFixed(3)) : 0,
  errorCount: errors.length,
  httpStatusCounts: Object.fromEntries([...statusCounts].sort(([a], [b]) => a - b)),
  errors,
  verifiedTaskId,
};
process.stdout.write(`${JSON.stringify(report)}\n`);
if (
  errors.length ||
  completion.length !== total ||
  report.acceptanceP95Ms > 500 ||
  report.documentP95Ms > 5000
)
  process.exitCode = 1;
