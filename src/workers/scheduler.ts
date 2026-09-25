import { promoteDueJobs } from "../queue";

// Delayed jobs can't be waited on with a blocking pop (Redis has no blocking
// "pop when score <= now"), so due retries are promoted on a short interval.
// This interval bounds how late a retry can fire past its backoff delay.
const PROMOTE_INTERVAL_MS = 500;

export class RetryScheduler {
  private timer: NodeJS.Timeout | null = null;
  private tickInFlight: Promise<void> | null = null;

  start(): void {
    if (this.timer) {
      return;
    }

    this.timer = setInterval(() => {
      if (this.tickInFlight) {
        return;
      }
      this.tickInFlight = this.tick().finally(() => {
        this.tickInFlight = null;
      });
    }, PROMOTE_INTERVAL_MS);
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    await this.tickInFlight;
  }

  private async tick(): Promise<void> {
    try {
      const promoted = await promoteDueJobs();
      if (promoted > 0) {
        console.log(`[scheduler] re-queued ${promoted} job(s) for retry`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.log(`[scheduler] error promoting delayed jobs: ${message}`);
    }
  }
}
