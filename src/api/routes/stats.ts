import { eq, sql } from "drizzle-orm";
import { Router } from "express";
import { db, jobs } from "../../db";
import { DEAD_LETTER_KEY, JOB_QUEUE_KEY, redis } from "../../queue";

export const statsRouter = Router();

statsRouter.get("/", async (_req, res, next) => {
  try {
    const [queueDepth, deadLetterCount, inProgressRow, completedRow] = await Promise.all([
      redis.llen(JOB_QUEUE_KEY),
      redis.llen(DEAD_LETTER_KEY),
      db
        .select({ count: sql<number>`count(*)::int` })
        .from(jobs)
        .where(eq(jobs.status, "processing")),
      db.select({ count: sql<number>`count(*)::int` }).from(jobs).where(eq(jobs.status, "completed")),
    ]);

    res.json({
      completed: completedRow[0]?.count ?? 0,
      queue_depth: queueDepth,
      in_progress: inProgressRow[0]?.count ?? 0,
      dead_letter: deadLetterCount,
    });
  } catch (error) { next(error); }
});
