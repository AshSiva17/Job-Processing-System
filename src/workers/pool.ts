import { and, eq, inArray, lt, sql } from "drizzle-orm";
import type Redis from "ioredis";
import { db, jobs } from "../db";
import { publishJobEvent } from "../events";
import { log, errorMessage } from "../logging";
import { blockingDequeueJob, createRedisConnection, moveToDeadLetter, scheduleRetry } from "../queue";
import { retryDelayMs } from "./backoff";
import { getHandler } from "./handlers";
import { startHeartbeat } from "./heartbeat";
import { JOB_TIMEOUT_MS, runWithTimeout } from "./timeout";

export class Worker {
  private connection: Redis | null = null;
  private running = false;
  private loopPromise: Promise<void> | null = null;
  constructor(private readonly id: number) {}

  start(): Promise<void> {
    if (this.loopPromise) return this.loopPromise;
    this.running = true;
    this.connection = createRedisConnection();
    this.emit("worker_started");
    this.loopPromise = this.loop();
    return this.loopPromise;
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.loopPromise;
    await this.connection?.quit();
    this.emit("worker_stopped");
  }

  private async loop(): Promise<void> {
    while (this.running && this.connection) {
      try {
        const jobId = await blockingDequeueJob(this.connection, 1);
        if (jobId) await this.processJob(jobId);
      } catch (error) {
        this.emit("worker_error", { error: errorMessage(error) });
        // Avoid a hot error loop during dependency outages. Processing rows
        // remain recoverable by the independent reaper.
        await new Promise(resolve => setTimeout(resolve, 250));
      }
    }
  }

  async processJob(jobId: string): Promise<void> {
    // Redis prevents taking one list entry twice; this conditional UPDATE also
    // prevents duplicate IDs from creating concurrent executions of one job.
    const [job] = await db.update(jobs).set({
      status: "processing", attempts: sql`${jobs.attempts} + 1`, updatedAt: sql`now()`,
    }).where(and(
      eq(jobs.id, jobId), inArray(jobs.status, ["pending", "failed"]),
      lt(jobs.attempts, jobs.maxAttempts),
    )).returning();
    if (!job) {
      this.emit("job_skipped", { job_id: jobId });
      return;
    }
    const ownership = and(eq(jobs.id, job.id), eq(jobs.status, "processing"), eq(jobs.attempts, job.attempts));
    this.emit("job_processing", { job_id: job.id, attempt: job.attempts });
    await publishJobEvent("processing", job);
    const stopHeartbeat = startHeartbeat(job.id, job.attempts, message =>
      this.emit("job_heartbeat", { job_id: job.id, attempt: job.attempts, message }));

    // Only handler errors are handler failures. A DB/queue outage after the
    // handler finishes must not accidentally count as a new handler failure.
    let failure: unknown;
    let failed = false;
    try {
      await runWithTimeout(signal => getHandler(job.type)(
        job.payload as Record<string, unknown>, signal, { attempt: job.attempts },
      ), JOB_TIMEOUT_MS);
    } catch (error) {
      failure = error;
      failed = true;
    } finally {
      await stopHeartbeat();
    }
    if (!failed) {
      const [completed] = await db.update(jobs)
        .set({ status: "completed", errorMessage: null }).where(ownership).returning();
      if (completed) {
        await publishJobEvent("completed", completed);
        this.emit("job_completed", { job_id: job.id, attempt: job.attempts });
      } else this.emit("job_ownership_lost", { job_id: job.id, attempt: job.attempts });
      return;
    }
    const dead = job.attempts >= job.maxAttempts;
    const [outcome] = await db.update(jobs).set({
      status: dead ? "dead" : "failed", errorMessage: errorMessage(failure),
    }).where(ownership).returning();
    if (!outcome) {
      this.emit("job_ownership_lost", { job_id: job.id, attempt: job.attempts });
      return;
    }
    const delayMs = dead ? 0 : retryDelayMs(job.attempts);
    if (dead) await moveToDeadLetter(job.id);
    else await scheduleRetry(job.id, delayMs);
    await publishJobEvent(dead ? "dead" : "failed", outcome);
    this.emit(dead ? "job_dead" : "job_retry_scheduled", {
      job_id: job.id, attempt: job.attempts, delay_ms: delayMs, error: errorMessage(failure),
    });
  }

  private emit(event: string, fields: Record<string, unknown> = {}): void {
    log(event, { worker_id: this.id, ...fields });
  }
}
