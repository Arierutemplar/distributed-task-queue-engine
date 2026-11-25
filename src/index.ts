import './config/dotenv.js';
import { loadSecrets, SecretsProviderError } from './config/secrets.js';

try {
  const loaded = await loadSecrets();
  await import('./config/env.js');
  const { logger } = await import('./config/logger.js');
  if (loaded.provider === 'vault') {
    logger.info({ provider: loaded.provider, keys: loaded.keys }, 'Runtime secrets loaded');
  }
  await import('./runtime.js');
} catch (error: unknown) {
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
