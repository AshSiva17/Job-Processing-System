import { createHash } from "node:crypto";
import type { NextFunction, Request, Response } from "express";
import { log } from "../../logging";
import { redis, QUEUE_NAMESPACE } from "../../queue";

// Max requests a client can make in a burst, and the steady-state rate at
// which spent tokens come back.
const CAPACITY = Number(process.env.RATE_LIMIT_BURST) || 20;
const REFILL_PER_SEC = Number(process.env.RATE_LIMIT_REFILL_PER_SEC) || 2;
const REFILL_PER_MS = REFILL_PER_SEC / 1000;

const KEY_PREFIX = `${QUEUE_NAMESPACE}rate-limit:create-job:`;

// Runs atomically so concurrent requests (across API instances) can't both
// spend the last token. Uses the Redis clock so API host clocks don't matter.
// Fractional token counts are returned as strings because Redis truncates Lua
// numbers to integers.
const TOKEN_BUCKET_SCRIPT = `
local capacity = tonumber(ARGV[1])
local refill_per_ms = tonumber(ARGV[2])
local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local bucket = redis.call("HMGET", KEYS[1], "tokens", "ts")
local tokens = tonumber(bucket[1]) or capacity
local ts = tonumber(bucket[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - ts) * refill_per_ms)

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call("HSET", KEYS[1], "tokens", tostring(tokens), "ts", now)
redis.call("PEXPIRE", KEYS[1], math.ceil(capacity / refill_per_ms))
return { allowed, tostring(tokens) }
`;

/**
 * Identifies the caller by `X-API-Key`, falling back to the client IP. Keys
 * are hashed so raw keys never appear in Redis.
 */
function clientId(req: Request): string {
  const apiKey = req.get("x-api-key");
  if (apiKey) {
    return `key:${createHash("sha256").update(apiKey).digest("hex")}`;
  }
  return `ip:${req.ip ?? "unknown"}`;
}

export async function rateLimitJobCreation(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  let allowed: boolean;
  let tokens: number;
  try {
    const [allowedFlag, remaining] = (await redis.eval(
      TOKEN_BUCKET_SCRIPT,
      1,
      KEY_PREFIX + clientId(req),
      CAPACITY,
      REFILL_PER_MS
    )) as [number, string];
    allowed = allowedFlag === 1;
    tokens = Number(remaining);
  } catch (error) {
    // Fail open: a limiter outage shouldn't take job creation down with it.
    const reason = error instanceof Error ? error.message : String(error);
    log("rate_limit_unavailable", { error: reason }, "warn");
    next();
    return;
  }

  res.set("RateLimit-Limit", String(CAPACITY));
  res.set("RateLimit-Remaining", String(Math.floor(tokens)));

  if (allowed) {
    next();
    return;
  }

  const retryAfterSec = Math.ceil((1 - tokens) / REFILL_PER_SEC);
  res.set("Retry-After", String(retryAfterSec));
  log("rate_limited", { client: clientId(req), retry_after_seconds: retryAfterSec });
  res.status(429).json({
    error: "Rate limit exceeded",
    retry_after_seconds: retryAfterSec,
  });
}
