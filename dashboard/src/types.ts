export type JobStatus =
  | "pending"
  | "processing"
  | "completed"
  | "failed"
  | "dead";

export interface Job {
  id: string;
  type: string;
  payload: unknown;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  created_at: string;
  updated_at: string;
  error_message: string | null;
}

export interface JobEvent {
  event: string;
  job: Job;
  timestamp: string;
}

export interface Stats {
  queue_depth: number;
  in_progress: number;
  dead_letter: number;
}
