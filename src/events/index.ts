import { formatJob } from "../api/utils/job";
import type { Job } from "../db";
import { log } from "../logging";
import { redis, QUEUE_NAMESPACE } from "../queue";

// Workers and the reaper run in separate processes from the API, so status
// changes travel over Redis pub/sub to reach the WebSocket server.
export const JOB_EVENTS_CHANNEL = `${QUEUE_NAMESPACE}job-events`;

export type JobEventType =
  | "retry"
  | "created"
  | "processing"
  | "completed"
  | "failed"
  | "dead";

export interface JobEvent {
  event: JobEventType;
  job: ReturnType<typeof formatJob>;
  timestamp: string;
}

/**
 * Publishes a job status change. Never throws: a broken event stream must not
 * fail the job itself, so errors are logged and swallowed.
 */
export async function publishJobEvent(
  event: JobEventType,
  job: Job | undefined
): Promise<void> {
  if (!job) {
    return;
  }

  const message: JobEvent = {
    event,
    job: formatJob(job),
    timestamp: new Date().toISOString(),
  };

  try {
    await redis.publish(JOB_EVENTS_CHANNEL, JSON.stringify(message));
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    log("event_publish_failed", { job_id: job.id, transition: event, error: reason }, "error");
  }
}
