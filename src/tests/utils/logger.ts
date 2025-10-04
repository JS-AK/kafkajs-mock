// Logging utilities for clear tracing
const nowIso = (): string => new Date().toISOString();
// eslint-disable-next-line no-console
export const logger = (testName: string) => (...args: unknown[]): void => console.log(`[TEST][${testName}]`, nowIso(), ...args);
