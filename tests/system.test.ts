import "dotenv/config";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import postgres from "postgres";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { spawn } from "node:child_process";
import type { Server } from "node:http";
import { WebSocket } from "ws";

let admin: ReturnType<typeof postgres>;
let data: typeof import("../src/db");
let queue: typeof import("../src/queue");
let Worker: typeof import("../src/workers/pool").Worker;
let Reaper: typeof import("../src/reaper/reaper").Reaper;
let heartbeat: typeof import("../src/workers/heartbeat");
let server: Server;
let closeEvents: () => Promise<void>;
let origin: string;
let name: string;
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => Promise<boolean>) {
  const deadline = Date.now() + 5000;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for condition");
    await pause(20);
  }
}
async function insert(type = "test-success", max = 3) {
  return (await data.client`INSERT INTO jobs(type,payload,max_attempts) VALUES (${type},'{}',${max}) RETURNING *`)[0];
}
async function row(id: string) { return (await data.client`SELECT * FROM jobs WHERE id=${id}`)[0]; }
async function post(body: unknown, key = randomUUID()) {
  return fetch(`${origin}/jobs`, { method: "POST", headers: {
    "Content-Type": "application/json", "X-API-Key": key,
  }, body: JSON.stringify(body) });
}

beforeAll(async () => {
  if (!process.env.DATABASE_URL || !process.env.REDIS_URL) throw new Error("Set DATABASE_URL and REDIS_URL for integration tests");
  name = `system_test_${process.pid}_${Date.now()}`;
  admin = postgres(process.env.DATABASE_URL, { max: 1 });
  await admin.unsafe(`CREATE DATABASE "${name}"`);
  const url = new URL(process.env.DATABASE_URL);
  url.pathname = `/${name}`;
  Object.assign(process.env, { DATABASE_URL: url.toString(), QUEUE_NAMESPACE: `${name}:`,
    HEARTBEAT_INTERVAL_MS: "20", PROCESSING_TIMEOUT_MS: "200", REAPER_INTERVAL_MS: "50",
    JOB_TIMEOUT_MS: "100", RETRY_BASE_DELAY_MS: "1", RETRY_MAX_DELAY_MS: "2",
    RATE_LIMIT_BURST: "3", RATE_LIMIT_REFILL_PER_SEC: "0.01", MOCK_FAILURE_RATE: "0" });
  data = await import("../src/db");
  const journal = JSON.parse(await readFile("drizzle/meta/_journal.json", "utf8"));
  for (const entry of journal.entries) {
    const migration = await readFile(`drizzle/${entry.tag}.sql`, "utf8");
    for (const statement of migration.split("--> statement-breakpoint")) {
      if (statement.trim()) await data.client.unsafe(statement);
    }
  }
  queue = await import("../src/queue");
  if (queue.redis.status !== "ready") await once(queue.redis, "ready");
  ({ Worker } = await import("../src/workers/pool"));
  ({ Reaper } = await import("../src/reaper/reaper"));
  heartbeat = await import("../src/workers/heartbeat");
  const { createApp } = await import("../src/api/app");
  server = createApp().listen(0, "127.0.0.1");
  await once(server, "listening");
  origin = `http://127.0.0.1:${(server.address() as {port:number}).port}`;
  closeEvents = (await import("../src/api/websocket")).attachJobEventsWebSocket(server);
  await pause(100);
});

beforeEach(async () => {
  vi.restoreAllMocks();
  if (data) await data.client`TRUNCATE jobs,job_attempts`;
  if (queue) {
    const keys = await queue.redis.keys(`${name}:*`);
    if (keys.length) await queue.redis.del(...keys);
  }
});
afterAll(async () => {
  vi.restoreAllMocks();
  if (closeEvents) await closeEvents();
  if (server) await new Promise<void>(resolve => server.close(() => resolve()));
  if (queue) {
    const keys = await queue.redis.keys(`${name}:*`);
    if (keys.length) await queue.redis.del(...keys);
    await queue.redis.quit();
  }
  if (data) await data.client.end();
  if (admin) {
    if (name) await admin.unsafe(`DROP DATABASE IF EXISTS "${name}"`);
    await admin.end();
  }
});

describe("finished-project checklist", () => {
  it.each([
    ["compiled", ["dist/workers/index.js"]],
    ["development", ["--import", "tsx", "src/workers/index.ts"]],
  ])("runs separate worker processes and recovers a real SIGKILL (%s)", async (_mode, args) => {
    const records: Record<string, any>[] = [];
    const pool = spawn(process.execPath, args, {
      env: { ...process.env, WORKER_COUNT: "2", JOB_TIMEOUT_MS: "5000" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let buffer = "";
    pool.stdout.on("data", chunk => {
      buffer += String(chunk);
      const lines = buffer.split("\n"); buffer = lines.pop()!;
      for (const line of lines) { try { records.push(JSON.parse(line)); } catch {} }
    });
    try {
      await until(async () => records.filter(r => r.event === "worker_started").length === 2);
      const pids = records.filter(r => r.event === "worker_started").map(r => r.process_id);
      expect(new Set(pids).size).toBe(2);
      expect(pids).not.toContain(pool.pid);
      const job = await insert("random-slow");
      await queue.enqueueJob(job.id);
      await until(async () => records.some(r => r.event === "job_processing" && r.job_id === job.id));
      const running = records.find(r => r.event === "job_processing" && r.job_id === job.id)!;
      process.kill(running.process_id, "SIGKILL");
      await pause(300); await new Reaper().sweep();
      expect((await row(job.id)).status).toBe("failed");
      await data.client`UPDATE jobs SET type='test-success' WHERE id=${job.id}`;
      await until(async () => (await row(job.id)).status === "completed");
      expect((await row(job.id)).attempts).toBe(2);
      expect(records.every(r => typeof r.timestamp === "string" && typeof r.event === "string")).toBe(true);
    } finally {
      const done = once(pool, "exit"); pool.kill("SIGTERM");
      const deadline = setTimeout(() => pool.kill("SIGKILL"), 8000);
      await done; clearTimeout(deadline);
    }
  });
  it("checks actual dependencies and names failures", async () => {
    const response = await fetch(`${origin}/health`);
    expect(response.status).toBe(200);
    expect((await response.json()).dependencies).toEqual({ postgres: "ok", redis: "ok" });
    const express = (await import("express")).default;
    const { createHealthRouter } = await import("../src/api/routes/health");
    const app = express();
    app.use(createHealthRouter({ postgres: async () => { throw new Error("offline"); }, redis: async () => {} }));
    const probe = app.listen(0, "127.0.0.1");
    await once(probe, "listening");
    try {
      const res = await fetch(`http://127.0.0.1:${(probe.address() as {port:number}).port}`);
      expect(res.status).toBe(503);
      expect((await res.json()).dependencies).toEqual({ postgres: "failed", redis: "ok" });
    } finally { await new Promise<void>(resolve => probe.close(() => resolve())); }
  });

  it("rejects invalid jobs without insertion; supports creation and unknown lookup", async () => {
    expect((await post({ type: "", payload: {} })).status).toBe(400);
    expect(Number((await data.client`SELECT count(*) FROM jobs`)[0].count)).toBe(0);
    const response = await post({ type: "test-success", payload: {} });
    expect(response.status).toBe(201);
    const job = await response.json();
    expect(job.status).toBe("pending");
    expect(await queue.redis.lrange(queue.JOB_QUEUE_KEY, 0, -1)).toEqual([job.id]);
    expect((await fetch(`${origin}/jobs/${job.id}`)).status).toBe(200);
    expect((await fetch(`${origin}/jobs/${randomUUID()}`)).status).toBe(404);
    const malformed = await fetch(`${origin}/jobs`, { method: "POST", headers: {"Content-Type":"application/json"}, body: "{" });
    expect(malformed.status).toBe(400);
  });

  it("uses FIFO ordering and prevents duplicate processing across two consumers", async () => {
    const a = await insert(); const b = await insert();
    await queue.enqueueJob(a.id); await queue.enqueueJob(b.id);
    const dedicated = queue.createRedisConnection();
    try {
      expect(await queue.blockingDequeueJob(dedicated, 1)).toBe(a.id);
      expect(await queue.blockingDequeueJob(dedicated, 1)).toBe(b.id);
    } finally { await dedicated.quit(); }
    const workers = [new Worker(1), new Worker(2)];
    const loops = workers.map(worker => worker.start());
    try {
      await Promise.all(Array.from({length: 10}, () => queue.enqueueJob(a.id)));
      await until(async () => (await row(a.id)).status === "completed");
      expect((await row(a.id)).attempts).toBe(1);
      expect(Number((await data.client`SELECT count(*) FROM job_attempts WHERE job_id=${a.id}`)[0].count)).toBe(1);
    } finally { await Promise.all(workers.map(worker => worker.stop())); await Promise.all(loops); }
  });

  it("retries with backoff then succeeds; exhausted jobs dead-letter only once", async () => {
    const worker = new Worker(1);
    const job = await insert("fail-then-succeed");
    await worker.processJob(job.id);
    expect((await row(job.id)).status).toBe("failed");
    expect(await queue.redis.zscore(queue.DELAYED_QUEUE_KEY, job.id)).not.toBeNull();
    await pause(5); expect(await queue.promoteDueJobs()).toBe(1);
    expect(await queue.redis.rpop(queue.JOB_QUEUE_KEY)).toBe(job.id);
    await worker.processJob(job.id);
    expect((await row(job.id)).status).toBe("completed");
    expect((await row(job.id)).attempts).toBe(2);
    const dead = await insert("always-fail", 2);
    await worker.processJob(dead.id);
    await pause(5); await queue.promoteDueJobs(); await queue.redis.rpop(queue.JOB_QUEUE_KEY);
    await worker.processJob(dead.id); await worker.processJob(dead.id);
    expect((await row(dead.id)).status).toBe("dead");
    expect((await row(dead.id)).attempts).toBe(2);
    expect(await queue.redis.lrange(queue.DEAD_LETTER_KEY, 0, -1)).toEqual([dead.id]);
  });

  it("keeps heartbeating jobs alive, reclaims silent jobs once, fences old heartbeats", async () => {
    const job = await insert();
    await data.client`UPDATE jobs SET status='processing',attempts=1 WHERE id=${job.id}`;
    const stop = heartbeat.startHeartbeat(job.id, 1, () => {});
    try {
      await pause(350); await new Reaper().sweep();
      expect((await row(job.id)).status).toBe("processing");
    } finally { await stop(); }
    await pause(250);
    await Promise.all([new Reaper().sweep(), new Reaper().sweep()]);
    expect((await row(job.id)).status).toBe("failed");
    expect(await queue.redis.zcard(queue.DELAYED_QUEUE_KEY)).toBe(1);
    await data.client`UPDATE jobs SET status='processing', attempts=2, updated_at=now()-interval '1 hour' WHERE id=${job.id}`;
    const oldHeartbeat = heartbeat.startHeartbeat(job.id, 1, () => {});
    try { await pause(60); expect(new Date((await row(job.id)).updated_at).getTime()).toBeLessThan(Date.now()-100000); }
    finally { await oldHeartbeat(); }
    await data.client`UPDATE jobs SET attempts=3, updated_at=now()-interval '1 hour' WHERE id=${job.id}`;
    await new Reaper().sweep();
    expect((await row(job.id)).status).toBe("dead");
  });

  it("times out a handler and routes it through failure/dead-letter", async () => {
    const job = await insert("random-slow", 1);
    await new Worker(1).processJob(job.id);
    expect((await row(job.id)).status).toBe("dead");
    expect((await row(job.id)).error_message).toContain("execution timeout");
  });

  it("fences an obsolete worker's completion after reaper and retry", async () => {
    const handlers = await import("../src/workers/handlers");
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    vi.spyOn(handlers, "getHandler").mockImplementationOnce(() => async () => gate);
    const job = await insert();
    const original = new Worker(1).processJob(job.id);
    await until(async () => (await row(job.id)).status === "processing");
    // Simulate recovery and a newer attempt while the old handler still exists.
    await data.client`UPDATE jobs SET status='failed' WHERE id=${job.id}`;
    await data.client`UPDATE jobs SET status='processing', attempts=2 WHERE id=${job.id}`;
    release(); await original;
    expect((await row(job.id)).status).toBe("processing");
    expect((await row(job.id)).attempts).toBe(2);
  });

  it("shares token buckets by API key and sends Retry-After", async () => {
    const key = randomUUID();
    const responses = await Promise.all(Array.from({length: 6}, () => post({type:"test-success",payload:{}}, key)));
    expect(responses.filter(res => res.status===201)).toHaveLength(3);
    expect(responses.filter(res => res.status===429)).toHaveLength(3);
    expect(Number(responses.find(res => res.status===429)!.headers.get("Retry-After"))).toBeGreaterThan(0);
    expect((await post({type:"test-success",payload:{}}, randomUUID())).status).toBe(201);
  });

  it("broadcasts retry, dead, processing and completion events to multiple clients", async () => {
    const sockets = [new WebSocket(origin.replace("http", "ws")+"/ws"), new WebSocket(origin.replace("http", "ws")+"/ws")];
    const received: string[][] = [[], []];
    sockets.forEach((socket, i) => socket.on("message", message => received[i].push(JSON.parse(String(message)).event)));
    try {
      await Promise.all(sockets.map(socket => once(socket, "open")));
      const response = await post({type:"fail-then-succeed",payload:{}});
      const job = await response.json();
      await new Worker(1).processJob(job.id);
      const { RetryScheduler } = await import("../src/workers/scheduler");
      const scheduler = new RetryScheduler(); scheduler.start();
      try { await until(async () => (await queue.redis.zcard(queue.DELAYED_QUEUE_KEY))===0); }
      finally { await scheduler.stop(); }
      await new Worker(1).processJob(job.id);
      const dead = await insert("always-fail",1); await new Worker(1).processJob(dead.id);
      await until(async () => received.every(events => events.includes("dead") && events.includes("completed") && events.includes("retry")));
      for (const events of received) expect(events).toEqual(expect.arrayContaining(["created","processing","failed","retry","completed","dead"]));
    } finally { sockets.forEach(socket => socket.terminate()); }
  });

  it("returns metrics and dashboard counts consistent with SQL", async () => {
    const worker = new Worker(1);
    const job = await insert(); await worker.processJob(job.id);
    const dead = await insert("always-fail",1); await worker.processJob(dead.id);
    const metrics = await (await fetch(`${origin}/metrics`)).json();
    expect(metrics.processed_attempts).toBe(2);
    expect(metrics.completed).toBe(1); expect(metrics.dead_letters).toBe(1);
    expect(metrics.jobs_processed_per_second).toBe(2/60);
    expect(metrics.dead_letter_rate).toBe(0.5);
    expect(metrics.average_processing_time_ms).toBeGreaterThan(0);
    const stats = await (await fetch(`${origin}/stats`)).json();
    expect(stats.completed).toBe(1); expect(stats.dead_letter).toBe(1);
  });
});
