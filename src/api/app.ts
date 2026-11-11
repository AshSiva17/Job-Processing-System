import express, { Express, ErrorRequestHandler } from "express";
import { healthRouter } from "./routes/health";
import { jobsRouter } from "./routes/jobs";
import { statsRouter } from "./routes/stats";

import { log, errorMessage } from "../logging";
import { metricsRouter } from "./routes/metrics";

export function createApp(): Express {
  const app = express();

  app.use((req, res, next) => {
    const started = Date.now();
    res.on("finish", () => log("http_request", {
      method: req.method, path: req.path, status: res.statusCode,
      duration_ms: Date.now() - started,
    }));
    next();
  });
  app.use(express.json());

  app.use("/health", healthRouter);
  app.use("/metrics", metricsRouter);
  app.use("/stats", statsRouter);
  app.use("/jobs", jobsRouter);

  const errors: ErrorRequestHandler = (error, _req, res, _next) => {
    const status = error.status === 400 ? 400 : 500;
    log("http_error", { error: errorMessage(error), status }, "error");
    res.status(status).json({ error: status === 400 ? "Invalid JSON body" : "Internal server error" });
  };
  app.use(errors);
  return app;
}
