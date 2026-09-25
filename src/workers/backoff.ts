const RETRY_BASE_DELAY_MS = Number(process.env.RETRY_BASE_DELAY_MS) || 1000;
const RETRY_MAX_DELAY_MS = Number(process.env.RETRY_MAX_DELAY_MS) || 60_000;

/**
 * Exponential backoff with full jitter: the delay ceiling doubles with each
 * attempt (base, 2x base, 4x base, ...) up to a cap, and the actual delay is
 * a random value below that ceiling so retries from many failing jobs don't
 * all land at the same moment.
 */
export function retryDelayMs(attempts: number): number {
  const ceiling = Math.min(
    RETRY_MAX_DELAY_MS,
    RETRY_BASE_DELAY_MS * 2 ** Math.max(0, attempts - 1)
  );
  return Math.round(Math.random() * ceiling);
}
