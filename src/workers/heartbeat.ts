import { and, eq, sql } from "drizzle-orm";
import { db, jobs } from "../db";

export const HEARTBEAT_INTERVAL_MS =
  Number(process.env.HEARTBEAT_INTERVAL_MS) || 10_000;

/**
 * Periodically bumps a processing job's `updated_at` so the reaper can tell
 * a job that is merely slow from one whose worker has died. Returns a
 * function that stops the heartbeat and waits for any in-flight beat.
 *
 * Timestamps use the database clock (`now()`) because the reaper compares
 * them against the database clock too; worker and reaper host clocks may
 * drift apart.
 */
export function startHeartbeat(
  jobId: string,
  attempt: number,
  log: (message: string) => void
): () => Promise<void> {
  let beatInFlight: Promise<void> | null = null;

  const beat = async (): Promise<void> => {
    try {
      const touched = await db
        .update(jobs)
        .set({ updatedAt: sql`now()` })
        .where(and(eq(jobs.id, jobId), eq(jobs.status, "processing"), eq(jobs.attempts, attempt)))
        .returning({ id: jobs.id });

      if (touched.length === 0) {
        log(
          `heartbeat: job ${jobId} is no longer 'processing' (likely reclaimed by the reaper)`
        );
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log(`heartbeat for job ${jobId} failed: ${message}`);
    }
  };

  const timer = setInterval(() => {
    if (beatInFlight) {
      return;
    }
    beatInFlight = beat().finally(() => {
      beatInFlight = null;
    });
  }, HEARTBEAT_INTERVAL_MS);

  return async () => {
    clearInterval(timer);
    await beatInFlight;
  };
}
