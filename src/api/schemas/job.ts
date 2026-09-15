import { z } from "zod";

export const createJobSchema = z.object({
  type: z.string().trim().min(1, "type is required"),
  payload: z.record(z.string(), z.unknown()),
  max_attempts: z.number().int().positive().default(3),
});

export type CreateJobInput = z.infer<typeof createJobSchema>;

export const jobIdParamSchema = z.string().uuid("Invalid job ID");
