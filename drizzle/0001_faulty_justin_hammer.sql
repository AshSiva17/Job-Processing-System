CREATE TABLE "job_attempts" (
	"job_id" uuid NOT NULL,
	"attempt" integer NOT NULL,
	"started_at" timestamp with time zone NOT NULL,
	"finished_at" timestamp with time zone,
	"outcome" "job_status",
	"duration_ms" double precision,
	CONSTRAINT "job_attempts_job_id_attempt_pk" PRIMARY KEY("job_id","attempt")
);
--> statement-breakpoint
CREATE INDEX "job_attempts_finished_at_idx" ON "job_attempts" USING btree ("finished_at");--> statement-breakpoint
CREATE FUNCTION record_job_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status = 'processing' AND
     (OLD.status <> 'processing' OR NEW.attempts <> OLD.attempts) THEN
    INSERT INTO job_attempts (job_id, attempt, started_at)
    VALUES (NEW.id, NEW.attempts, clock_timestamp())
    ON CONFLICT DO NOTHING;
  ELSIF OLD.status = 'processing' AND NEW.status IN ('completed', 'failed', 'dead') THEN
    UPDATE job_attempts
    SET finished_at = clock_timestamp(), outcome = NEW.status,
        duration_ms = GREATEST(0, EXTRACT(EPOCH FROM (clock_timestamp() - started_at)) * 1000)
    WHERE job_id = NEW.id AND attempt = OLD.attempts AND finished_at IS NULL;
  END IF;
  RETURN NEW;
END;
$$;
--> statement-breakpoint
CREATE TRIGGER jobs_record_attempt
AFTER UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION record_job_attempt();
