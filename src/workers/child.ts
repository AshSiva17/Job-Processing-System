import "dotenv/config";
import { Worker } from "./pool";
import { client } from "../db";
import { redis } from "../queue";
import { log, errorMessage } from "../logging";

const worker = new Worker(Number(process.env.WORKER_ID));
let stopping = false;
async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
  await worker.stop();
  await Promise.all([client.end(), redis.quit()]);
  process.exit(0);
}
process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
void worker.start().catch(error => {
  log("worker_crashed", { worker_id: Number(process.env.WORKER_ID), error: errorMessage(error) }, "error");
  process.exit(1);
});
