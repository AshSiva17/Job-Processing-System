require('dotenv/config');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFile } = require('node:fs/promises');
const postgres = require('postgres');
const { drizzle } = require('drizzle-orm/postgres-js');
const express = require('express');
const { getMetrics } = require('../dist/metrics');
const { createMetricsRouter } = require('../dist/api/routes/metrics');

// Requires a local Postgres user allowed to CREATE DATABASE. All fixtures and
// migrations run in a fresh database, never against the application's jobs.
test('metrics agree with attempt history and expose correct HTTP results', async () => {
  assert.ok(process.env.DATABASE_URL, 'Set DATABASE_URL before running this test');
  const name = `metrics_test_${process.pid}_${Date.now()}`;
  const admin = postgres(process.env.DATABASE_URL, { max: 1 });
  let client;
  let server;
  let created = false;
  try {
    await admin.unsafe(`CREATE DATABASE "${name}"`);
    created = true;
    const url = new URL(process.env.DATABASE_URL);
    url.pathname = `/${name}`;
    client = postgres(url.toString(), { max: 1 });
    for (const file of ['0000_big_taskmaster.sql', '0001_faulty_justin_hammer.sql']) {
      const migration = await readFile(`${__dirname}/../drizzle/${file}`, 'utf8');
      for (const statement of migration.split('--> statement-breakpoint')) {
        if (statement.trim()) await client.unsafe(statement);
      }
    }
    const database = drizzle(client);
    assert.deepEqual(await getMetrics(database), {
      window_seconds: 60, processed_attempts: 0, completed: 0, retries: 0,
      dead_letters: 0, jobs_processed_per_second: 0,
      average_processing_time_ms: 0, retry_rate: 0, dead_letter_rate: 0,
    });
    const [job] = await client`INSERT INTO jobs (type,payload,max_attempts)
      VALUES ('metrics-test','{}',3) RETURNING id`;
    // Three real lifecycle transitions: retry, retry, then exhausted/dead.
    // Use known start times to independently check durations despite heartbeats.
    for (const [attempt, outcome, duration] of [[1,'failed',1000],[2,'failed',2000],[3,'dead',3000]]) {
      await client`UPDATE jobs SET status='processing', attempts=${attempt} WHERE id=${job.id}`;
      await client`UPDATE job_attempts SET started_at=clock_timestamp() - ${duration} * interval '1 millisecond'
        WHERE job_id=${job.id} AND attempt=${attempt}`;
      await client`UPDATE jobs SET updated_at=clock_timestamp() WHERE id=${job.id}`;
      await client`UPDATE jobs SET status=${outcome}::job_status WHERE id=${job.id}`;
    }
    const [success] = await client`INSERT INTO jobs (type,payload,max_attempts)
      VALUES ('metrics-test','{}',3) RETURNING id`;
    await client`UPDATE jobs SET status='processing', attempts=1 WHERE id=${success.id}`;
    await client`UPDATE job_attempts SET started_at=clock_timestamp()-interval '4 seconds'
      WHERE job_id=${success.id}`;
    await client`UPDATE jobs SET status='completed' WHERE id=${success.id}`;
    // Exclude a pending attempt, an unfinished attempt, and an old outcome.
    await client`INSERT INTO jobs (type,payload,max_attempts) VALUES ('pending','{}',3)`;
    const [active] = await client`INSERT INTO jobs (type,payload,max_attempts) VALUES ('active','{}',3) RETURNING id`;
    await client`UPDATE jobs SET status='processing', attempts=1 WHERE id=${active.id}`;
    await client`INSERT INTO job_attempts (job_id,attempt,started_at,finished_at,outcome,duration_ms)
      VALUES (gen_random_uuid(),1,now()-interval '2 hours',now()-interval '1 hour','completed',999999)`;

    const app = express();
    app.use('/metrics', createMetricsRouter(() => getMetrics(database)));
    server = await new Promise(resolve => {
      const listener = app.listen(0, '127.0.0.1', () => resolve(listener));
    });
    const response = await fetch(`http://127.0.0.1:${server.address().port}/metrics`);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    const metrics = await response.json();
    assert.equal(metrics.processed_attempts, 4);
    assert.equal(metrics.completed, 1);
    assert.equal(metrics.retries, 2);
    assert.equal(metrics.dead_letters, 1);
    assert.equal(metrics.jobs_processed_per_second, 4/60);
    assert.equal(metrics.retry_rate, 0.5);
    assert.equal(metrics.dead_letter_rate, 0.25);
    assert.ok(Math.abs(metrics.average_processing_time_ms - 2500) < 500);
    const [manual] = await client`SELECT count(*)::int AS n, avg(duration_ms)::float8 AS mean
      FROM job_attempts WHERE finished_at > now()-interval '60 seconds'`;
    assert.equal(metrics.processed_attempts, manual.n);
    assert.equal(metrics.average_processing_time_ms, manual.mean);
    console.log('Cross-checked API against SQL and four known outcomes:', metrics);
  } finally {
    if (server) await new Promise(resolve => server.close(resolve));
    if (client) await client.end();
    if (created) await admin.unsafe(`DROP DATABASE "${name}"`);
    await admin.end();
  }
});
