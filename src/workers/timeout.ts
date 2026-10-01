export const JOB_TIMEOUT_MS = Number(process.env.JOB_TIMEOUT_MS) || 60_000;

export class JobTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`Job exceeded execution timeout of ${timeoutMs}ms`);
    this.name = "JobTimeoutError";
  }
}

/**
 * Runs `run` with a deadline. On timeout the signal passed to `run` is
 * aborted (so cooperative handlers can stop early) and the returned promise
 * rejects with JobTimeoutError regardless of whether `run` honours the
 * signal. A handler that ignores the signal keeps running in the background,
 * so handlers with side effects should check it.
 */
export async function runWithTimeout<T>(
  run: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number
): Promise<T> {
  const controller = new AbortController();
  let timer: NodeJS.Timeout | undefined;

  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      const error = new JobTimeoutError(timeoutMs);
      controller.abort(error);
      reject(error);
    }, timeoutMs);
  });

  try {
    return await Promise.race([
      Promise.resolve().then(() => run(controller.signal)),
      deadline,
    ]);
  } finally {
    clearTimeout(timer);
  }
}
