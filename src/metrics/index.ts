import { sql } from "drizzle-orm";
import { db } from "../db";

// A fixed rolling window keeps denominators comparable across API processes
// and makes rates decay to zero when the system becomes idle.
export const METRICS_WINDOW_SECONDS = 60;

export async function getMetrics(database: Pick<typeof db, "execute"> = db) {
  const rows = await database.execute(sql`
    SELECT count(*)::int AS processed_attempts,
           count(*) FILTER (WHERE outcome = 'completed')::int AS completed,
           count(*) FILTER (WHERE outcome = 'failed')::int AS retries,
           count(*) FILTER (WHERE outcome = 'dead')::int AS dead_letters,
           coalesce(avg(duration_ms), 0)::float8 AS average_processing_time_ms
    FROM job_attempts
    WHERE finished_at > statement_timestamp() - interval '60 seconds'
      AND finished_at <= statement_timestamp()
  `);
  const row = rows[0];
  const processed = Number(row.processed_attempts);
  const retries = Number(row.retries);
  const dead = Number(row.dead_letters);
  return {
    window_seconds: METRICS_WINDOW_SECONDS,
    processed_attempts: processed,
    completed: Number(row.completed),
    retries,
    dead_letters: dead,
    jobs_processed_per_second: processed / METRICS_WINDOW_SECONDS,
    average_processing_time_ms: Number(row.average_processing_time_ms),
    retry_rate: processed === 0 ? 0 : retries / processed,
    dead_letter_rate: processed === 0 ? 0 : dead / processed,
  };
}
