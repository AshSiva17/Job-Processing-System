import Redis from "ioredis";

const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  throw new Error("REDIS_URL is not set");
}

export const redis = new Redis(redisUrl);

export const JOB_QUEUE_KEY = "job-queue";

export async function enqueueJob(jobId: string): Promise<void> {
  await redis.rpush(JOB_QUEUE_KEY, jobId);
}
