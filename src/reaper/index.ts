import "dotenv/config";
import { Reaper } from "./reaper";

const reaper = new Reaper();

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  console.log(`\nReceived ${signal}, shutting down reaper...`);
  await reaper.stop();
  console.log("Reaper stopped");
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

reaper.start();
