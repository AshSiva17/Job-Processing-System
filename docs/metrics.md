# Job metrics

Run `npm run db:migrate`, restart the API, then request `GET /metrics`:

```sh
curl -s http://localhost:3000/metrics | python3 -m json.tool
```

The JSON endpoint uses a rolling 60-second window, based on the Postgres clock.
All API and worker instances share the same durable attempt history. A database
trigger records the start and finish of each attempt in the same transaction as
the job status update, including timeout failures and reaper recovery. Heartbeats
do not reset the attempt start time. API restarts do not reset the metrics.

- `processed_attempts`: attempts that finished in the window, including failures.
- `completed`: attempts that completed successfully.
- `retries`: attempts ending in retryable `failed` status (retry decisions, not proof
  that Redis accepted the retry or that the next execution has started).
- `dead_letters`: attempts ending in `dead` status (dead-letter decisions).
- `jobs_processed_per_second`: `processed_attempts / 60`. Counts attempts, so a
  job with retries can contribute more than once. This is not successful-job throughput.
- `average_processing_time_ms`: mean start-to-finish duration of those attempts.
  Excludes queue waiting and retry backoff, includes handler timeouts and, for
  crashed attempts, the elapsed time until the reaper detects them.
- `retry_rate`: retryable failures divided by finished attempts, a fraction from 0 to 1.
- `dead_letter_rate`: dead outcomes divided by finished attempts, a fraction from 0 to 1.

Empty windows return zero for every metric. Historical attempts before this
migration are not reconstructed: the jobs table cannot establish their timings
or individual outcomes. Jobs already processing when it is applied will not be
measured until a later attempt starts. No existing job data is changed.

## Verification

`npm run test:metrics` builds and runs an integration test using `DATABASE_URL`.
The Postgres user must be allowed to create a temporary database. The test applies
both migrations there, exercises job transitions and heartbeats, checks empty and
populated HTTP responses, and compares the API to SQL. The database is removed
in cleanup; the application's jobs and Redis queues are untouched.

The controlled sample has four finished attempts: one completion, two retryable
failures, and one dead letter, with durations near 1, 2, 3, and 4 seconds. Expected:
4/60 attempts/sec, approximately 2500ms average, retry rate 0.5, dead-letter rate
0.25. Pending/in-flight attempts and old history must not affect these numbers.

For a manual cross-check (run promptly after fetching the endpoint; the rolling
window can otherwise move past an event):

```sql
SELECT count(*) AS processed_attempts,
       count(*) / 60.0 AS jobs_processed_per_second,
       coalesce(avg(duration_ms), 0) AS average_processing_time_ms,
       coalesce(count(*) FILTER (WHERE outcome = 'failed')::numeric
                / nullif(count(*), 0), 0) AS retry_rate,
       coalesce(count(*) FILTER (WHERE outcome = 'dead')::numeric
                / nullif(count(*), 0), 0) AS dead_letter_rate
FROM job_attempts
WHERE finished_at > statement_timestamp() - interval '60 seconds'
  AND finished_at <= statement_timestamp();
```

This endpoint measures persisted database transitions. Postgres updates and Redis
queue writes are separate operations in the current architecture, so the metrics
cannot prove Redis delivery or exactly-once execution. Attempt history currently
has no retention policy; arrange archival if it grows large.
