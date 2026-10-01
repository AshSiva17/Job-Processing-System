import type { Server } from "http";
import { WebSocket, WebSocketServer } from "ws";
import { JOB_EVENTS_CHANNEL } from "../events";
import { createRedisConnection } from "../queue";

export const WEBSOCKET_PATH = "/ws";

// Clients that miss a ping for this long are terminated, so half-open TCP
// connections don't accumulate.
const PING_INTERVAL_MS = 30_000;

/**
 * Attaches a WebSocket server to `server` that relays every message on the
 * job events channel to all connected clients. Returns a function that
 * closes client connections and the Redis subscription.
 */
export function attachJobEventsWebSocket(server: Server): () => Promise<void> {
  const wss = new WebSocketServer({ server, path: WEBSOCKET_PATH });
  const alive = new WeakMap<WebSocket, boolean>();

  wss.on("connection", (socket) => {
    alive.set(socket, true);
    socket.on("pong", () => alive.set(socket, true));
    socket.on("error", (error) => {
      console.log(`[ws] client error: ${error.message}`);
    });
  });

  const pingTimer = setInterval(() => {
    for (const socket of wss.clients) {
      if (!alive.get(socket)) {
        socket.terminate();
        continue;
      }
      alive.set(socket, false);
      socket.ping();
    }
  }, PING_INTERVAL_MS);

  // A connection in subscriber mode can't run other commands, so it gets
  // its own rather than sharing the queue connection.
  const subscriber = createRedisConnection();

  subscriber.on("message", (channel, message) => {
    if (channel !== JOB_EVENTS_CHANNEL) {
      return;
    }
    for (const socket of wss.clients) {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(message);
      }
    }
  });

  subscriber.subscribe(JOB_EVENTS_CHANNEL).catch((error) => {
    const reason = error instanceof Error ? error.message : String(error);
    console.log(`[ws] failed to subscribe to ${JOB_EVENTS_CHANNEL}: ${reason}`);
  });

  return async () => {
    clearInterval(pingTimer);
    for (const socket of wss.clients) {
      socket.close(1001, "Server shutting down");
    }
    await new Promise<void>((resolve) => wss.close(() => resolve()));
    await subscriber.quit();
  };
}
