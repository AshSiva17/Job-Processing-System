import Redis from "ioredis";
import { log } from "../logging";

export const QUEUE_NAMESPACE = process.env.QUEUE_NAMESPACE ?? "";

const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  throw new Error("REDIS_URL is not set");
}

export const JOB_QUEUE_KEY = `${QUEUE_NAMESPACE}job-queue`;
// Sorted set of job IDs waiting out a retry backoff, scored by the epoch-ms
// timestamp at which they become eligible to run again.
export const DELAYED_QUEUE_KEY = `${QUEUE_NAMESPACE}job-delayed`;
export const DEAD_LETTER_KEY = `${QUEUE_NAMESPACE}job-dead-letter`;

// Shared connection for simple, non-blocking commands (e.g. enqueueing).
function monitor(connection: Redis): Redis {
  connection.on("error", error => log("redis_error", { error: error.message }, "error"));
  return connection;
}

export const redis = monitor(new Redis(redisUrl, {
  maxRetriesPerRequest: 1, commandTimeout: 5000, enableOfflineQueue: false,
}));

/**
 * Creates a dedicated Redis connection. Blocking commands (like BRPOP) tie
 * up the connection they run on, so each worker needs its own connection
 * rather than sharing the one above.
 */
export function createRedisConnection(): Redis {
  return monitor(new Redis(redisUrl as string));
}

export async function enqueueJob(jobId: string): Promise<void> {
  await redis.lpush(JOB_QUEUE_KEY, jobId);
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

export async function scheduleRetry(
  jobId: string,
  delayMs: number
): Promise<void> {
  await redis.zadd(DELAYED_QUEUE_KEY, Date.now() + delayMs, jobId);
}

export async function moveToDeadLetter(jobId: string): Promise<void> {
  await redis.rpush(DEAD_LETTER_KEY, jobId);
}

// Runs atomically inside Redis so that concurrent promoters (e.g. several
// worker processes) can never move the same job onto the queue twice.
const PROMOTE_DUE_JOBS_SCRIPT = `
local due = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[2])
for _, jobId in ipairs(due) do
  redis.call("ZREM", KEYS[1], jobId)
  redis.call("LPUSH", KEYS[2], jobId)
end
return due
`;

/**
 * Moves delayed jobs whose backoff has elapsed back onto the main queue.
 * Resolves with the IDs promoted, for lifecycle events and structured logs.
 */
export async function promoteDueJobIds(batchSize = 100): Promise<string[]> {
  const promoted = await redis.eval(
    PROMOTE_DUE_JOBS_SCRIPT,
    2,
    DELAYED_QUEUE_KEY,
    JOB_QUEUE_KEY,
    Date.now(),
    batchSize
  );
  return promoted as string[];
}

export async function promoteDueJobs(batchSize = 100): Promise<number> {
  return (await promoteDueJobIds(batchSize)).length;
}
