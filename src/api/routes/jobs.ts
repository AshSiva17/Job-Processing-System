import { desc, eq } from "drizzle-orm";
import { Router } from "express";
import { db, jobs } from "../../db";
import { publishJobEvent } from "../../events";
import { enqueueJob } from "../../queue";
import { rateLimitJobCreation } from "../middleware/rateLimit";
import { createJobSchema, jobIdParamSchema } from "../schemas/job";
import { formatJob } from "../utils/job";

export const jobsRouter = Router();

jobsRouter.post("/", rateLimitJobCreation, async (req, res) => {
  const parsed = createJobSchema.safeParse(req.body);

  if (!parsed.success) {
    res.status(400).json({
      error: "Validation failed",
      details: parsed.error.flatten().fieldErrors,
    });
    return;
  }

  const { type, payload, max_attempts } = parsed.data;

  const [job] = await db
    .insert(jobs)
    .values({
      type,
      payload,
      maxAttempts: max_attempts,
      status: "pending",
    })
    .returning();

  await publishJobEvent("created", job);
  await enqueueJob(job.id);

  res.status(201).json(formatJob(job));
});

const MAX_LIST_LIMIT = 500;

jobsRouter.get("/", async (req, res) => {
  const parsed = Number(req.query.limit);
  const limit = Number.isFinite(parsed)
    ? Math.min(Math.max(1, Math.floor(parsed)), MAX_LIST_LIMIT)
    : 100;

  const rows = await db
    .select()
    .from(jobs)
    .orderBy(desc(jobs.createdAt))
    .limit(limit);

  res.json({ jobs: rows.map(formatJob) });
});

jobsRouter.get("/:id", async (req, res) => {
  const parsedId = jobIdParamSchema.safeParse(req.params.id);

  if (!parsedId.success) {
    res.status(400).json({ error: "Invalid job ID" });
    return;
  }

  const job = await db.query.jobs.findFirst({
    where: eq(jobs.id, parsedId.data),
  });

  if (!job) {
    res.status(404).json({ error: "Job not found" });
    return;
  }

  res.json(formatJob(job));
});
