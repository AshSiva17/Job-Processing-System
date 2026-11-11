# Finished-project checklist review

Reviewed against the full component checklist on November 10, 2026.

## Infrastructure and storage

- Compose: Postgres/Redis ports, named volumes, dependency health checks,
  environment defaults; Redis AOF enabled in Compose.
- `.env.example` is present and `.env` is ignored; existing `.env` is preserved.
- TypeScript source folders and `tsx watch` workflows are present.
- `/health` now checks actual dependencies and names failures with 503; readiness
  responds within a deadline rather than hanging on an unavailable dependency.
- Jobs schema has all requested columns and statuses. Attempt-history migration
  adds persistent metrics without fabricating old processing times.
- Main FIFO queue uses LPUSH/BRPOP; delayed retries use a sorted set; final jobs
  use the separate dead-letter list; token buckets live in Redis.

## API and processing

- POST validation, default attempts, enqueueing, ID lookup and list route checked.
  Malformed JSON returns 400; rejected payloads produce no rows.
- Redis token bucket is per API key (IP fallback) and returns Retry-After on 429.
- Pool now forks N worker processes instead of running N loops in one process;
  a supervisor restarts crashed children.
- Conditional SQL claims prevent concurrent duplicate-ID processing. Attempt
  fencing protects heartbeats/outcomes against an obsolete worker.
- Registry now includes deterministic always-fail, fail-then-succeed and
  success handlers, alongside the random abortable mock.
- Exponential retry has full jitter; attempts count starts exactly once and
  stop at max_attempts. Terminal jobs are skipped and never auto-retried.
- Reaper, database-clock heartbeat and cooperative execution timeout are present.

## Real-time, observability and quality

- Pub/sub broadcasts all persisted transitions; scheduler also emits retry events.
- Multiple WebSocket clients and clean disconnects tested. Dashboard reloads the
  job snapshot on reconnect and now includes completed count.
- Structured JSON logs cover API, workers, reaper, scheduler, limiter and WebSocket
  errors. Job-scoped lifecycle logs include UUID, attempt and worker identity.
- `/metrics` has durable cross-process state, explicit 60-second definitions,
  raw counts, and independent SQL verification.
- Vitest integration suite: 12 passing scenarios, including actual SIGKILL
  recovery, duplicate queue IDs, stale-attempt fencing, retries/dead letters,
  heartbeat silence, timeout, rate limiting and multi-client WebSockets.
- Independent metrics integration test passes; backend typecheck/build and
  dashboard production build pass.
- Both labeled k6 profiles measured against isolated temporary state; raw results
  and SQL checks are saved. Full-duration profile remains available but unmeasured.
- README now covers setup, data flow, design decisions, metrics definitions,
  measured benchmarks and known limitations.
- Compatible dependency audit fixes applied. Tooling advisories that require a
  breaking change are documented rather than force-downgraded.

## Boundaries of completion

The checklist does not imply exactly-once external effects or transactionally
reliable Redis delivery. DB/Redis dual writes and the pop-before-claim gap still
need an outbox/reconciliation mechanism for production guarantees. The README
states this explicitly, along with cancellation and pub/sub limitations.

Existing staged changes were preserved. Historical conventional commits cannot
be reconstructed truthfully from this review; no past history was rewritten.
