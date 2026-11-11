import { Router } from "express";
import { sql } from "drizzle-orm";
import { db } from "../../db";
import { redis } from "../../queue";

async function probe(check: () => Promise<unknown>): Promise<"ok" | "failed"> {
  let timer: NodeJS.Timeout | undefined;
  try {
    await Promise.race([
      Promise.resolve().then(check),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("Readiness timeout")), 2000);
      }),
    ]);
    return "ok";
  } catch {
    return "failed";
  } finally {
    clearTimeout(timer);
  }
}

export function createHealthRouter(checks = {
  postgres: () => db.execute(sql`SELECT 1`),
  redis: () => redis.ping().then(reply => {
    if (reply !== "PONG") throw new Error("Unexpected Redis response");
  }),
}): Router {
  const router = Router();
  router.get("/", async (_req, res) => {
    const [postgres, redis] = await Promise.all([probe(checks.postgres), probe(checks.redis)]);
    const ready = postgres === "ok" && redis === "ok";
    res.status(ready ? 200 : 503).json({
      status: ready ? "ok" : "unhealthy", dependencies: { postgres, redis },
      timestamp: new Date().toISOString(),
    });
  });
  return router;
}
export const healthRouter = createHealthRouter();
