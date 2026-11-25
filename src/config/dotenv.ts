import dotenv from 'dotenv';

// Keep developer .env files out of hermetic tests.
if (process.env.VITEST === undefined) {
  dotenv.config({ quiet: true });
}
