import { and, eq, lt, sql } from "drizzle-orm";
import { db, jobs } from "../db";
import { publishJobEvent } from "../events";
import { moveToDeadLetter, scheduleRetry } from "../queue";
import { retryDelayMs } from "../workers/backoff";
import { HEARTBEAT_INTERVAL_MS } from "../workers/heartbeat";

// Workers bump `updated_at` every HEARTBEAT_INTERVAL_MS while a job runs, so
// this only needs to span a few missed heartbeats, not the longest job.
const PROCESSING_TIMEOUT_MS =
  Number(process.env.PROCESSING_TIMEOUT_MS) || 30_000;
const REAPER_INTERVAL_MS = Number(process.env.REAPER_INTERVAL_MS) || 10_000;

const STALE_ERROR_MESSAGE = `Worker went silent: no heartbeat for over ${PROCESSING_TIMEOUT_MS}ms`;

export class Reaper {
  private timer: NodeJS.Timeout | null = null;
  private sweepInFlight: Promise<void> | null = null;

  start(): void {
    if (this.timer) {
      return;
    }

    console.log(
      `[reaper] started (timeout=${PROCESSING_TIMEOUT_MS}ms, heartbeat=${HEARTBEAT_INTERVAL_MS}ms, interval=${REAPER_INTERVAL_MS}ms)`
    );

    if (PROCESSING_TIMEOUT_MS < 2 * HEARTBEAT_INTERVAL_MS) {
      console.log(
        `[reaper] warning: PROCESSING_TIMEOUT_MS should be at least 2x HEARTBEAT_INTERVAL_MS, or a single late heartbeat will get a live job reaped`
      );
    }

    void this.runSweep();
    this.timer = setInterval(() => void this.runSweep(), REAPER_INTERVAL_MS);
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    await this.sweepInFlight;
  }

  private runSweep(): Promise<void> {
    if (!this.sweepInFlight) {
      this.sweepInFlight = this.sweep()
        .catch((error) => {
          const message = error instanceof Error ? error.message : String(error);
          console.log(`[reaper] sweep failed: ${message}`);
        })
        .finally(() => {
          this.sweepInFlight = null;
        });
    }
    return this.sweepInFlight;
  }

  /**
   * Claims stale jobs and routes them like an ordinary failure: retry with
   * backoff if attempts remain, otherwise dead-letter. The crashed attempt was
   * already counted when the worker picked the job up, so a job that keeps
   * crashing its worker still runs out of attempts instead of looping forever.
   */
  private async sweep(): Promise<void> {
    // Computed on the database clock to match the heartbeat writes.
    const cutoff = sql`now() - make_interval(secs => ${PROCESSING_TIMEOUT_MS / 1000})`;

    // A single conditional UPDATE claims each row atomically, so two reapers
    // running at once can't both re-queue the same job.
    const reaped = await db
      .update(jobs)
      .set({
        status: sql`CASE WHEN ${jobs.attempts} >= ${jobs.maxAttempts} THEN 'dead'::job_status ELSE 'failed'::job_status END`,
        errorMessage: STALE_ERROR_MESSAGE,
      })
      .where(and(eq(jobs.status, "processing"), lt(jobs.updatedAt, cutoff)))
      .returning();

    for (const job of reaped) {
      if (job.status === "dead") {
        await moveToDeadLetter(job.id);
        await publishJobEvent("dead", job);
        console.log(
          `[reaper] job ${job.id} stuck on final attempt ${job.attempts}/${job.maxAttempts}, moved to dead-letter`
        );
        continue;
      }

      const delayMs = retryDelayMs(job.attempts);
      await scheduleRetry(job.id, delayMs);
      await publishJobEvent("failed", job);
      console.log(
        `[reaper] job ${job.id} stuck on attempt ${job.attempts}/${job.maxAttempts}, retrying in ${delayMs}ms`
      );
    }
  }
}
