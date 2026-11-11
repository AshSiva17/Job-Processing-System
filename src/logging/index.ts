type Fields = Record<string, unknown>;

export function log(event: string, fields: Fields = {}, level = "info"): void {
  console.log(JSON.stringify({ job_id: null, worker_id: null, process_id: process.pid, ...fields, timestamp: new Date().toISOString(), level, event }));
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
