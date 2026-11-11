import "dotenv/config";
import { fork, ChildProcess } from "node:child_process";
import { extname, join } from "node:path";
import { once } from "node:events";
import { RetryScheduler } from "./scheduler";
import { JOB_TIMEOUT_MS } from "./timeout";
import { client } from "../db";
import { redis } from "../queue";
import { log } from "../logging";

const count = Number(process.env.WORKER_COUNT ?? 4);
if (!Number.isInteger(count) || count < 1 || count > 256) {
  throw new Error("WORKER_COUNT must be an integer between 1 and 256");
}
const children = new Map<number, ChildProcess>();
const scheduler = new RetryScheduler();
let shuttingDown = false;

function start(id: number): void {
  if (shuttingDown) return;
  // fork inherits tsx's loader in development; compiled production uses .js.
  const child = fork(join(__dirname, `child${extname(__filename)}`), [], {
    env: { ...process.env, WORKER_ID: String(id) }, stdio: "inherit",
  });
  children.set(id, child);
  log("worker_spawned", { worker_id: id, pid: child.pid });
  child.on("exit", (code, signal) => {
    children.delete(id);
    log("worker_exited", { worker_id: id, code, signal });
    if (!shuttingDown) setTimeout(() => start(id), 1000);
  });
}

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  log("worker_pool_stopping", { signal });
  await scheduler.stop();
  await Promise.all([...children.values()].map(async child => {
    const done = once(child, "exit");
    child.kill("SIGTERM");
    const deadline = setTimeout(() => child.kill("SIGKILL"), JOB_TIMEOUT_MS + 10_000);
    await done;
    clearTimeout(deadline);
  }));
  await Promise.all([client.end(), redis.quit()]);
  log("worker_pool_stopped");
  process.exit(0);
}
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
log("worker_pool_started", { worker_count: count });
scheduler.start();
for (let id = 1; id <= count; id++) start(id);
