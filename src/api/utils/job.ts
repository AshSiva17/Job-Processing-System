import { Job } from "../../db";

export function formatJob(job: Job) {
  return {
    id: job.id,
    type: job.type,
    payload: job.payload,
    status: job.status,
    attempts: job.attempts,
    max_attempts: job.maxAttempts,
    created_at: job.createdAt.toISOString(),
    updated_at: job.updatedAt.toISOString(),
    error_message: job.errorMessage,
  };
}
