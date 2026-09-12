import express, { Express } from "express";
import { healthRouter } from "./routes/health";

export function createApp(): Express {
  const app = express();

  app.use(express.json());

  app.use("/health", healthRouter);

  return app;
}
