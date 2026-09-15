import { eq } from "drizzle-orm";
import { Router } from "express";
import { db, jobs } from "../../db";
import { enqueueJob } from "../../queue";
import { createJobSchema, jobIdParamSchema } from "../schemas/job";
import { formatJob } from "../utils/job";

export const jobsRouter = Router();

jobsRouter.post("/", async (req, res) => {
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

  await enqueueJob(job.id);

  res.status(201).json(formatJob(job));
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
