// Mock job handlers, keyed by job `type`. These simulate work by sleeping
// for a random duration and occasionally throwing to exercise the failure
// path. Replace entries here with real business logic per job type later.

import { setTimeout as sleep } from "node:timers/promises";

// `signal` is aborted when the job exceeds its execution timeout. Handlers
// should pass it to cancellable APIs (fetch, timers, DB queries) or check
// `signal.aborted` between steps so they stop doing work once timed out.
export type JobHandler = (
  payload: Record<string, unknown>,
  signal: AbortSignal
) => Promise<void>;

const MIN_DURATION_MS = 500;
const MAX_DURATION_MS = 3000;

// Probability (0-1) that the mock handler simulates a failure.
const MOCK_FAILURE_RATE = Number(process.env.MOCK_FAILURE_RATE ?? 0.1);

function randomDuration(): number {
  return MIN_DURATION_MS + Math.random() * (MAX_DURATION_MS - MIN_DURATION_MS);
}

async function mockHandler(
  _payload: Record<string, unknown>,
  signal: AbortSignal
): Promise<void> {
  const durationMs = randomDuration();
  await sleep(durationMs, undefined, { signal });

  if (Math.random() < MOCK_FAILURE_RATE) {
    throw new Error(
      `Mock handler failure (simulated after ${Math.round(durationMs)}ms)`
    );
  }
}

// No type-specific handlers yet — every job type currently falls back to
// the mock handler below.
const handlers: Record<string, JobHandler> = {};

export function getHandler(jobType: string): JobHandler {
  return handlers[jobType] ?? mockHandler;
}
