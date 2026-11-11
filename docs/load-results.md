# Measured local load results

Measured November 10, 2026, America/Toronto, using `npm run load:local`.
Raw UTC measurement timestamps are retained in the verification JSON files.
Environment: macOS arm64, 8 logical CPUs, Node 22.16.0, local Postgres 16 and
Redis 7 containers, and Docker k6. Image digest:
`sha256:3e4186aea62e11a45560bfc51e19eeafe82fc07ce069c78aff44b7ff893d9a5e`.

Both runs used 16 separate worker processes and the deterministic `test-success`
handler (20ms sleep). Each offered a 30-second ramp of 0 → 100 → 200 → 100
submissions/sec. Each run had its own temporary database and Redis namespace.
These are short local workload measurements, not a maximum sustainable capacity
claim or a completed run of the default 6.5-minute profile.

## Throughput run

`K6_PER_VU_API_KEY=true`, 2000-token burst and refill 1000/sec per key to keep
rate limits out of the processing-capacity measurement:

- 3500 submissions accepted; all 3500 completed, independently verified in SQL.
- Observed accepted throughput: **115.48 jobs/sec**, including the k6 drain period.
- POST latency p95: **4.31ms**.
- End-to-end completion p95: **260ms** (includes polling; not handler time).
- No rate-limited requests, completion timeouts, dropped iterations, or unfinished rows.

Sources: [raw k6 summary](../load/results/throughput-summary.json) and
[SQL verification and settings](../load/results/throughput-verification.json).

## Rate-limit run

`K6_PER_VU_API_KEY=false`, one shared key, normal 20-token burst and 2 tokens/sec:

- 79 submissions accepted and completed; **3420 rejected with 429**.
- Observed accepted throughput: **2.61 jobs/sec**, including the initial burst and drain.
- POST latency p95: **20.84ms**, across accepted and rejected requests.
- End-to-end completion p95 for accepted jobs: **335ms**.
- No completion timeouts, dropped iterations, or unfinished rows.
- SQL independently matched the 79 accepted/completed jobs.

79 accepted is consistent with about 20 initial tokens plus about 60 replenished
tokens over 30 seconds. The first submission occurs after the ramp begins and
the final refill may not have reached a full token before submissions end.

Sources: [raw k6 summary](../load/results/rate-limit-summary.json) and
[SQL verification and settings](../load/results/rate-limit-verification.json).

## Interpreting the measurements

The throughput run uses a generous limiter; the rate-limit run deliberately
uses ordinary settings. Per-VU keys do not themselves guarantee that a heavily
reused VU won't exhaust its bucket. The script applies different thresholds to
the two profiles so expected 429s do not fail the limiter test.

`/metrics` uses a fixed rolling 60-second denominator and counts finished
attempts, whereas k6's accepted rate uses its measured run duration. They should
not be compared directly: 3500 attempts completed within one metrics window
would produce 3500/60 = 58.33 attempts/sec even though this k6 run reports
115.48 accepted/sec over about 30.3 seconds. Both are correct for their windows.
The automated metrics test checks raw attempt counts, ratios, and durations
against direct SQL and a known controlled workload.
