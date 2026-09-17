import "dotenv/config";
import { Worker } from "./pool";

const WORKER_COUNT = Number(process.env.WORKER_COUNT) || 4;

const workers: Worker[] = Array.from(
  { length: WORKER_COUNT },
  (_unused, index) => new Worker(index + 1)
);

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  console.log(`\nReceived ${signal}, shutting down worker pool...`);
  await Promise.all(workers.map((worker) => worker.stop()));
  console.log("Worker pool stopped");
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

console.log(`Starting worker pool with ${WORKER_COUNT} worker(s)`);

Promise.all(workers.map((worker) => worker.start())).catch((error) => {
  console.error("Worker pool crashed:", error);
  process.exit(1);
});
