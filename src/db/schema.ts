import {
  integer,
  doublePrecision,
  index,
  primaryKey,
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uuid,
} from "drizzle-orm/pg-core";

export const jobStatusEnum = pgEnum("job_status", [
  "pending",
  "processing",
  "completed",
  "failed",
  "dead",
]);

export const jobs = pgTable("jobs", {
  id: uuid("id").primaryKey().defaultRandom(),
  type: text("type").notNull(),
  payload: jsonb("payload").notNull(),
  status: jobStatusEnum("status").notNull().default("pending"),
  attempts: integer("attempts").notNull().default(0),
  maxAttempts: integer("max_attempts").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true })
    .notNull()
    .defaultNow()
    .$onUpdate(() => new Date()),
  errorMessage: text("error_message"),
});

export type Job = typeof jobs.$inferSelect;
export type NewJob = typeof jobs.$inferInsert;
export type JobStatus = (typeof jobStatusEnum.enumValues)[number];

// Durable per-attempt history. A database trigger records transitions from
// workers and the reaper in the same transaction as the job status change.
export const jobAttempts = pgTable("job_attempts", {
  jobId: uuid("job_id").notNull(),
  attempt: integer("attempt").notNull(),
  startedAt: timestamp("started_at", { withTimezone: true }).notNull(),
  finishedAt: timestamp("finished_at", { withTimezone: true }),
  outcome: jobStatusEnum("outcome"),
  durationMs: doublePrecision("duration_ms"),
}, (table) => [
  primaryKey({ columns: [table.jobId, table.attempt] }),
  index("job_attempts_finished_at_idx").on(table.finishedAt),
]);
