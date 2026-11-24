// Throwaway credentials used only by the integration suite.
export const KEY_A1 = 'it-tenant-a-key-1-0123456789abcdef01234567';
export const KEY_A2 = 'it-tenant-a-key-2-0123456789abcdef01234567';
export const KEY_B = 'it-tenant-b-key-0123456789abcdef0123456789';
export const TENANT_A = 'tenant-a';
export const API_KEYS = `${TENANT_A}:${KEY_A1},${TENANT_A}:${KEY_A2},${KEY_B}`;

export const ADMIN_USERNAME = 'operator';
export const ADMIN_PASSWORD = 'integration-admin-password';

export const SERVICE_NAME = 'dtq-it';
export const TEST_REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6380';
