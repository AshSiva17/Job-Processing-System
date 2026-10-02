import express, { Express } from "express";
import { healthRouter } from "./routes/health";
import { jobsRouter } from "./routes/jobs";
import { statsRouter } from "./routes/stats";

export function createApp(): Express {
  const app = express();

  app.use(express.json());

  app.use("/health", healthRouter);
  app.use("/stats", statsRouter);
  app.use("/jobs", jobsRouter);

  return app;
}
