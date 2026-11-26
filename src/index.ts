import './config/dotenv.js';
import { createSecretsProvider, SecretsProviderError } from './config/secrets.js';

const secrets = createSecretsProvider((event) => {
  process.stderr.write(
    `${JSON.stringify({ level: event.outcome === 'success' ? 30 : 50, msg: 'Vault token lifecycle', ...event })}\n`,
  );
});
try {
  const initialEnvironment = { ...process.env };
  const loaded = await secrets.load(process.env);
  await import('./config/env.js');
  const { logger } = await import('./config/logger.js');
  if (loaded.provider === 'vault') {
    logger.info({ provider: loaded.provider, keys: loaded.keys }, 'Runtime secrets loaded');
  }
  const { startRuntime } = await import('./runtime.js');
  startRuntime(initialEnvironment, secrets);
} catch (error: unknown) {
  await secrets.close();
  const detail =
    error instanceof SecretsProviderError ||
    (error instanceof Error && error.name === 'ConfigurationError')
      ? error.message
      : 'Runtime initialization failed';
  process.stderr.write(
    `${JSON.stringify({ level: 60, msg: 'Startup configuration failed', detail })}\n`,
  );
  process.exitCode = 1;
}
