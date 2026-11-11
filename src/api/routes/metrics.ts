import { Router } from "express";
import { getMetrics } from "../../metrics";

export function createMetricsRouter(readMetrics = getMetrics): Router {
  const router = Router();
  router.get("/", async (_req, res, next) => {
    try {
      res.set("Cache-Control", "no-store").json(await readMetrics());
    } catch (error) {
      next(error);
    }
  });
  return router;
}

export const metricsRouter = createMetricsRouter();
