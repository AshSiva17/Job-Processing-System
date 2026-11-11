// Isolated local benchmarks: never points the load generator at existing jobs.
require('dotenv/config');
const postgres = require('postgres');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { mkdtemp, readFile, mkdir, rm } = require('node:fs/promises');
const { openSync, closeSync } = require('node:fs');
const { tmpdir, cpus, platform, arch } = require('node:os');
const { join } = require('node:path');
const net = require('node:net');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function freePort() {
  const server = net.createServer().listen(0, '127.0.0.1');
  await once(server, 'listening'); const port = server.address().port;
  await new Promise(resolve => server.close(resolve)); return port;
}
async function stop(child) {
  if (!child || child.exitCode !== null || child.signalCode) return;
  const done = once(child, 'exit'); child.kill('SIGTERM');
  const timer = setTimeout(() => child.kill('SIGKILL'), 8000);
  await done; clearTimeout(timer);
}
async function run(label) {
  const name = `load_test_${label.replace('-','_')}_${process.pid}_${Date.now()}`;
  const admin = postgres(process.env.DATABASE_URL, {max:1});
  const scratch = await mkdtemp(join(tmpdir(), 'jobs-load-'));
  const url = new URL(process.env.DATABASE_URL); url.pathname = `/${name}`;
  const port = await freePort();
  const env = {...process.env, DATABASE_URL:url.toString(), PORT:String(port),
    QUEUE_NAMESPACE:`${name}:`, WORKER_COUNT:'16', MOCK_FAILURE_RATE:'0',
    // Throughput isolates API/worker capacity; the second run exercises the
    // normal single-key 20-token burst + 2 token/sec limiter.
    RATE_LIMIT_BURST:label==='throughput'?'2000':'20',
    RATE_LIMIT_REFILL_PER_SEC:label==='throughput'?'1000':'2'};
  let api, worker, db, created = false;
  const fds = [];
  try {
    await admin.unsafe(`CREATE DATABASE "${name}"`); created=true;
    db=postgres(url.toString(), {max:1});
    const journal=JSON.parse(await readFile('drizzle/meta/_journal.json','utf8'));
    for(const entry of journal.entries) {
      const sql=await readFile(`drizzle/${entry.tag}.sql`,'utf8');
      for(const statement of sql.split('--> statement-breakpoint')) if(statement.trim()) await db.unsafe(statement);
    }
    const start = (script, logfile) => {
      const fd=openSync(join(scratch, logfile),'w'); fds.push(fd);
      return spawn(process.execPath,[script],{env,stdio:['ignore',fd,fd]});
    };
    api=start('dist/index.js','api.log'); worker=start('dist/workers/index.js','worker.log');
    let ready=false;
    for(let i=0;i<100;i++) {
      try { if((await fetch(`http://127.0.0.1:${port}/health`)).ok) {ready=true;break;} } catch {}
      await delay(100);
    }
    if(!ready) throw new Error('Isolated API not ready');
    const output=`load/results/${label}-summary.json`;
    await mkdir('load/results',{recursive:true});
    const args=['run','--rm','--user',`${process.getuid()}:${process.getgid()}`,
      '-v',`${process.cwd()}:/work`,'-w','/work',
      '-e',`BASE_URL=http://host.docker.internal:${port}`,
      '-e',`K6_PER_VU_API_KEY=${label==='throughput'}`,
      '-e','K6_SHORT_RUN=true','-e','JOB_TYPE=test-success',
      '-e',`API_KEY=${name}`,'-e',`K6_SUMMARY_PATH=/work/${output}`,
      'grafana/k6:latest','run','load/k6/submit-jobs.js'];
    console.log(`Starting ${label}: 30 seconds, 0 → 100 → 200 → 100 submissions/sec`);
    const k6=spawn('docker',args,{stdio:'inherit'});
    const [code]=await once(k6,'exit');
    if(code!==0) throw new Error(`k6 ${label} failed with exit ${code}`);
    const summary=JSON.parse(await readFile(output,'utf8'));
    const [counts]=await db`SELECT count(*)::int AS total,
      count(*) FILTER(WHERE status='completed')::int AS completed,
      count(*) FILTER(WHERE status IN ('pending','processing','failed'))::int AS unfinished FROM jobs`;
    const accepted=summary.metrics.jobs_accepted.values.count;
    if(counts.total!==accepted || counts.completed!==accepted || counts.unfinished!==0) {
      throw new Error(`SQL mismatch: ${JSON.stringify(counts)} vs accepted ${accepted}`);
    }
    const {writeFile}=require('node:fs/promises');
    await writeFile(`load/results/${label}-verification.json`,JSON.stringify({
      measured_at:new Date().toISOString(),platform:platform(),arch:arch(),cpus:cpus().length,
      node:process.version,worker_count:16,handler:'test-success (20ms sleep)',
      rate_limit_burst:env.RATE_LIMIT_BURST,rate_limit_refill_per_sec:env.RATE_LIMIT_REFILL_PER_SEC,
      profile:'30-second ramp: 0 → 100 → 200 → 100 submissions/sec',
      accepted,rate_limited:summary.metrics.jobs_rate_limited?.values.count||0,
      accepted_per_second:summary.metrics.jobs_accepted.values.rate,
      submit_p95_ms:summary.metrics.job_submit_latency.values['p(95)'],
      completion_p95_ms:summary.metrics.job_completion_time?.values['p(95)'],
      dropped_iterations:summary.metrics.dropped_iterations?.values.count||0,
      completion_timeouts:summary.metrics.job_completion_timeouts?.values.count||0,sql:counts,
    },null,2)+'\n');
    console.log(`${label}: SQL agrees with ${accepted} accepted/completed jobs`);
  } finally {
    await Promise.all([stop(api),stop(worker)]);
    for(const fd of fds) closeSync(fd);
    const Redis=require('ioredis'); const redis=new Redis(process.env.REDIS_URL);
    try {
      const keys=await redis.keys(`${name}:*`); if(keys.length) await redis.del(...keys);
    } finally {await redis.quit();}
    if(db) await db.end();
    if(created) await admin.unsafe(`DROP DATABASE "${name}"`);
    await admin.end(); await rm(scratch,{recursive:true,force:true});
  }
}
(async()=>{for(const label of ['throughput','rate-limit']) await run(label);})().catch(error=>{console.error(error);process.exitCode=1;});
