import Redis from "ioredis";

const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  throw new Error("REDIS_URL is not set");
}

export const JOB_QUEUE_KEY = "job-queue";

// Shared connection for simple, non-blocking commands (e.g. enqueueing).
export const redis = new Redis(redisUrl);

/**
 * Creates a dedicated Redis connection. Blocking commands (like BRPOP) tie
 * up the connection they run on, so each worker needs its own connection
 * rather than sharing the one above.
 */
export function createRedisConnection(): Redis {
  return new Redis(redisUrl as string);
}

export async function enqueueJob(jobId: string): Promise<void> {
  await redis.rpush(JOB_QUEUE_KEY, jobId);
}

/**
 * Blocking pop from the job queue. Resolves with the job ID, or `null` if
 * `timeoutSeconds` elapses with no job available.
 */
export async function blockingDequeueJob(
  connection: Redis,
  timeoutSeconds: number
): Promise<string | null> {
  const result = await connection.brpop(JOB_QUEUE_KEY, timeoutSeconds);

  if (!result) {
    return null;
  }

  const [, jobId] = result;
  return jobId;
}
