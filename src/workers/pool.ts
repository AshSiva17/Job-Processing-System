import { eq } from "drizzle-orm";
import type Redis from "ioredis";
import { db, jobs } from "../db";
import { blockingDequeueJob, createRedisConnection } from "../queue";
import { getHandler } from "./handlers";

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

    this.log(`picked up job ${job.id} (type=${job.type})`);

    await db
      .update(jobs)
      .set({ status: "processing", attempts: job.attempts + 1 })
      .where(eq(jobs.id, job.id));

    try {
      const handler = getHandler(job.type);
      await handler(job.payload as Record<string, unknown>);

      await db
        .update(jobs)
        .set({ status: "completed", errorMessage: null })
        .where(eq(jobs.id, job.id));

      this.log(`job ${job.id} completed`);
    } catch (error) {
      const message = describeError(error);

      await db
        .update(jobs)
        .set({ status: "failed", errorMessage: message })
        .where(eq(jobs.id, job.id));

      this.log(`job ${job.id} failed: ${message}`);
    }
  }

  private log(message: string): void {
    console.log(`[worker ${this.id}] ${message}`);
  }
}

function describeError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
