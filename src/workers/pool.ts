import { eq, sql } from "drizzle-orm";
import type Redis from "ioredis";
import { db, jobs } from "../db";
import { publishJobEvent } from "../events";
import {
  blockingDequeueJob,
  createRedisConnection,
  moveToDeadLetter,
  scheduleRetry,
} from "../queue";
import { retryDelayMs } from "./backoff";
import { getHandler } from "./handlers";
import { startHeartbeat } from "./heartbeat";
import { JOB_TIMEOUT_MS, runWithTimeout } from "./timeout";

// How long each BRPOP call blocks before timing out and looping again.
// A finite timeout (rather than 0/infinite) lets the worker notice
// `stop()` requests in a timely manner.
const BRPOP_TIMEOUT_SECONDS = 5;

export class Worker {
  private readonly id: number;
  private connection: Redis | null = null;
  private running = false;
  private loopPromise: Promise<void> | null = null;

  constructor(id: number) {
    this.id = id;
  }

  start(): Promise<void> {
    if (this.loopPromise) {
      return this.loopPromise;
    }

    this.running = true;
    this.connection = createRedisConnection();
    this.log("started, waiting for jobs");

    this.loopPromise = this.loop();
    return this.loopPromise;
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.loopPromise;
    await this.connection?.quit();
  }

  private async loop(): Promise<void> {
    while (this.running) {
      const connection = this.connection;
      if (!connection) {
        break;
      }

      let jobId: string | null = null;
      try {
        jobId = await blockingDequeueJob(connection, BRPOP_TIMEOUT_SECONDS);
      } catch (error) {
        this.log(`error while polling queue: ${describeError(error)}`);
        continue;
      }

      // BRPOP timed out with nothing in the queue — loop and try again.
      if (!jobId) {
        continue;
      }

      await this.processJob(jobId);
    }
  }

  private async processJob(jobId: string): Promise<void> {
    const job = await db.query.jobs.findFirst({
      where: eq(jobs.id, jobId),
    });

    if (!job) {
      this.log(`job ${jobId} not found in database, skipping`);
      return;
    }

    if (job.status === "completed" || job.status === "dead") {
      this.log(`job ${job.id} is already ${job.status}, skipping`);
      return;
    }

    const attempts = job.attempts + 1;
    this.log(
      `picked up job ${job.id} (type=${job.type}, attempt ${attempts}/${job.maxAttempts})`
    );

    const [processing] = await db
      .update(jobs)
      .set({ status: "processing", attempts, updatedAt: sql`now()` })
      .where(eq(jobs.id, job.id))
      .returning();
    await publishJobEvent("processing", processing);

    const stopHeartbeat = startHeartbeat(job.id, (message) =>
      this.log(message)
    );

    try {
      const handler = getHandler(job.type);
      try {
        await runWithTimeout(
          (signal) => handler(job.payload as Record<string, unknown>, signal),
          JOB_TIMEOUT_MS
        );
      } finally {
        await stopHeartbeat();
      }

      const [completed] = await db
        .update(jobs)
        .set({ status: "completed", errorMessage: null })
        .where(eq(jobs.id, job.id))
        .returning();
      await publishJobEvent("completed", completed);

      this.log(`job ${job.id} completed`);
    } catch (error) {
      await this.handleFailure(job.id, attempts, job.maxAttempts, error);
    }
  }

  private async handleFailure(
    jobId: string,
    attempts: number,
    maxAttempts: number,
    error: unknown
  ): Promise<void> {
    const message = describeError(error);

    if (attempts >= maxAttempts) {
      const [dead] = await db
        .update(jobs)
        .set({ status: "dead", errorMessage: message })
        .where(eq(jobs.id, jobId))
        .returning();
      await moveToDeadLetter(jobId);
      await publishJobEvent("dead", dead);

      this.log(
        `job ${jobId} failed on final attempt ${attempts}/${maxAttempts}, moved to dead-letter: ${message}`
      );
      return;
    }

    const delayMs = retryDelayMs(attempts);

    const [failed] = await db
      .update(jobs)
      .set({ status: "failed", errorMessage: message })
      .where(eq(jobs.id, jobId))
      .returning();
    await scheduleRetry(jobId, delayMs);
    await publishJobEvent("failed", failed);

    this.log(
      `job ${jobId} failed on attempt ${attempts}/${maxAttempts}, retrying in ${delayMs}ms: ${message}`
    );
  }

  private log(message: string): void {
    console.log(`[worker ${this.id}] ${message}`);
  }
}

function describeError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
