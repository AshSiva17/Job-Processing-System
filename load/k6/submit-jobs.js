/**
 * Load test: submit ~6,900 jobs at varying arrival rates, measure POST latency
 * and time until each job reaches a terminal state (completed or dead).
 *
 * Prerequisites:
 *   - API running (default http://localhost:3000)
 *   - Workers running (npm run worker:dev) for completion metrics
 *   - k6 installed: https://grafana.com/docs/k6/latest/set-up/install-k6/
 *
 * Run:
 *   k6 run load/k6/submit-jobs.js
 *
 * Env:
 *   BASE_URL          API origin (default http://localhost:3000)
 *   API_KEY           X-API-Key header value (default k6-load)
 *   K6_PER_VU_API_KEY  If "true", append -${VU} so each VU has its own rate-limit bucket
 *   JOB_TYPE          Job type string (default k6-load)
 *   MAX_ATTEMPTS      max_attempts field (default 3)
 *   POLL_INTERVAL     Seconds between GET /jobs/:id polls (default 0.25)
 *   COMPLETION_TIMEOUT  Max seconds to wait per job after submit (default 600)
 *
 * Rate limiting: POST /jobs uses a token bucket per API key. High arrival rates
 * will produce 429s unless you raise RATE_LIMIT_* on the server or set
 * K6_PER_VU_API_KEY=true so each VU uses a distinct key.
 */

import { check, sleep } from "k6";
import http from "k6/http";
import { Counter, Rate, Trend } from "k6/metrics";


const BASE_URL = __ENV.BASE_URL || "http://localhost:3000";
const JOB_TYPE = __ENV.JOB_TYPE || "k6-load";
const MAX_ATTEMPTS = Number(__ENV.MAX_ATTEMPTS || 3);
const POLL_INTERVAL = Number(__ENV.POLL_INTERVAL || 0.25);
const COMPLETION_TIMEOUT_SEC = Number(__ENV.COMPLETION_TIMEOUT || 600);
const API_KEY_BASE = __ENV.API_KEY || "k6-load";
const PER_VU_API_KEY =
  __ENV.K6_PER_VU_API_KEY === "1" || __ENV.K6_PER_VU_API_KEY === "true";

const submitLatency = new Trend("job_submit_latency", true);
const completionTime = new Trend("job_completion_time", true);
const jobsAccepted = new Counter("jobs_accepted");
const jobsCompleted = new Counter("jobs_completed");
const jobsDead = new Counter("jobs_dead");
const completionTimeouts = new Counter("job_completion_timeouts");
const rateLimited = new Counter("jobs_rate_limited");
const submitOk = new Rate("job_submit_success_rate");

const RUN_LABEL = PER_VU_API_KEY ? "throughput" : "rate-limit";
const SHORT_RUN = __ENV.K6_SHORT_RUN === "true";
const TERMINAL = new Set(["completed", "dead"]);

function apiKey() {
  return PER_VU_API_KEY ? `${API_KEY_BASE}-${__VU}` : API_KEY_BASE;
}

function headers(json = true) {
  const h = { "X-API-Key": apiKey() };
  if (json) {
    h["Content-Type"] = "application/json";
  }
  return h;
}

/**
 * Ramping arrival rate — integrated rate ≈ 6,900 POST attempts over ~6.5 min:
 *   warm 5/s → ramp 35/s → sustain 10/s → spike 50/s → cool 3/s
 */
export const options = {
  scenarios: {
    varying_submit: {
      executor: "ramping-arrival-rate",
      startRate: 0,
      timeUnit: "1s",
      preAllocatedVUs: 80,
      maxVUs: 250,
      stages: SHORT_RUN ? [
        { duration: "10s", target: 100 },
        { duration: "10s", target: 200 },
        { duration: "10s", target: 100 },
      ] : [
        { duration: "1m", target: 5 },
        { duration: "2m", target: 35 },
        { duration: "1m", target: 10 },
        { duration: "45s", target: 50 },
        { duration: "45s", target: 3 },
        { duration: "30s", target: 15 },
        { duration: "30s", target: 0 },
      ],
      gracefulStop: "60s",
      exec: "submitAndWait",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    // Expected 429s are not transport failures in the limiter profile.
    ...(PER_VU_API_KEY ? { job_submit_success_rate: ["rate>0.99"] }
      : { jobs_rate_limited: ["count>0"] }),
    job_completion_timeouts: ["count==0"],
    job_submit_latency: ["p(95)<500"],
  },
};

export function setup() {
  completionTimeouts.add(0);
  jobsAccepted.add(0);
  rateLimited.add(0);
  http.setResponseCallback(http.expectedStatuses(200, 201, 429));
  const res = http.get(`${BASE_URL}/health`);
  check(res, {
    "API health ok": (r) => r.status === 200,
  });
  if (res.status !== 200) {
    throw new Error(`API not reachable at ${BASE_URL} (GET /health → ${res.status})`);
  }
}

export function submitAndWait() {
  http.setResponseCallback(http.expectedStatuses(200, 201, 429));
  const submitStart = Date.now();
  const payload = JSON.stringify({
    type: JOB_TYPE,
    payload: {
      k6_vu: __VU,
      k6_iter: __ITER,
      ts: submitStart,
    },
    max_attempts: MAX_ATTEMPTS,
  });

  const postRes = http.post(`${BASE_URL}/jobs`, payload, {
    headers: headers(),
    tags: { name: "POST /jobs" },
  });

  const accepted = postRes.status === 201;
  submitOk.add(accepted);
  submitLatency.add(postRes.timings.duration);

  if (postRes.status === 429) {
    rateLimited.add(1);
    return;
  }

  if (!accepted) {
    return;
  }

  jobsAccepted.add(1);
  rateLimited.add(0);

  let job;
  try {
    job = postRes.json();
  } catch {
    return;
  }

  if (!job?.id) {
    return;
  }

  const deadline = submitStart + COMPLETION_TIMEOUT_SEC * 1000;

  while (Date.now() < deadline) {
    const getRes = http.get(`${BASE_URL}/jobs/${job.id}`, {
      headers: headers(false),
      tags: { name: "GET /jobs/:id" },
    });

    if (getRes.status === 200) {
      let current;
      try {
        current = getRes.json();
      } catch {
        sleep(POLL_INTERVAL);
        continue;
      }

      if (TERMINAL.has(current.status)) {
        const elapsed = Date.now() - submitStart;
        completionTime.add(elapsed);
        if (current.status === "completed") {
          jobsCompleted.add(1);
        } else {
          jobsDead.add(1);
        }
        return;
      }
    }

    sleep(POLL_INTERVAL);
  }

  completionTimeouts.add(1);
}

export function handleSummary(data) {
  const accepted = data.metrics.jobs_accepted?.values?.count ?? 0;
  const limited = data.metrics.jobs_rate_limited?.values?.count ?? 0;
  const completed = data.metrics.jobs_completed?.values?.count ?? 0;
  const dead = data.metrics.jobs_dead?.values?.count ?? 0;
  const timeouts = data.metrics.job_completion_timeouts?.values?.count ?? 0;

  const submitP95 = data.metrics.job_submit_latency?.values?.["p(95)"];
  const completionP95 = data.metrics.job_completion_time?.values?.["p(95)"];

  const extra = [
    "",
    `─── ${RUN_LABEL} job processing load test ───`,
    `  Accepted submissions/sec: ${data.metrics.jobs_accepted?.values?.rate ?? 0}`,
    `  POST accepted (201):     ${accepted}`,
    `  Rate limited (429):      ${limited}`,
    `  Completed:               ${completed}`,
    `  Dead:                    ${dead}`,
    `  Completion timeouts:     ${timeouts}`,
    submitP95 != null
      ? `  Submit latency p95:      ${submitP95.toFixed(2)} ms`
      : null,
    completionP95 != null
      ? `  Completion time p95:     ${(completionP95 / 1000).toFixed(2)} s`
      : null,
    "",
  ]
    .filter(Boolean)
    .join("\n");

  return {
    stdout: extra + "\n",
    [__ENV.K6_SUMMARY_PATH || `load/k6/${RUN_LABEL}-summary.json`]: JSON.stringify(data, null, 2),
  };
}
