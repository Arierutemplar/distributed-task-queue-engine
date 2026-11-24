import { Router } from 'express';

export interface ReadinessCheck {
  name: string;
  check: () => Promise<void> | void;
}

export interface ReadinessReport {
  status: 'ready' | 'not_ready';
  checks: Record<string, { status: 'up' | 'down'; error?: string }>;
}

type CheckResult = ReadinessReport['checks'][string];

export const evaluateReadiness = async (checks: ReadinessCheck[]): Promise<ReadinessReport> => {
  const results = await Promise.all(
    checks.map(async ({ name, check }): Promise<[string, CheckResult]> => {
      try {
        await check();
        return [name, { status: 'up' }];
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        return [name, { status: 'down', error: message }];
      }
    }),
  );
  const report = Object.fromEntries(results);
  const ready = results.every(([, result]) => result.status === 'up');
  return { status: ready ? 'ready' : 'not_ready', checks: report };
};

export const createHealthRouter = (readinessChecks: ReadinessCheck[]): Router => {
  const router = Router();

  // Liveness: the event loop answers. Never depends on Redis, so an outage does not restart pods.
  router.get('/live', (_request, response) => {
    response.json({ status: 'ok', uptimeSeconds: Math.round(process.uptime()) });
  });

  router.get('/ready', async (_request, response) => {
    const report = await evaluateReadiness(readinessChecks);
    response.status(report.status === 'ready' ? 200 : 503).json(report);
  });

  return router;
};
