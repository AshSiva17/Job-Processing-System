import "dotenv/config";
import { log, errorMessage } from "../logging";
import { Reaper } from "./reaper";

const reaper = new Reaper();

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  log("reaper_stopping", { signal });
  await reaper.stop();
  log("reaper_stopped");
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

reaper.start();
