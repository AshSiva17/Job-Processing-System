import "dotenv/config";
import { log } from "./logging";
import { createApp } from "./api/app";
import { attachJobEventsWebSocket, WEBSOCKET_PATH } from "./api/websocket";

const PORT = Number(process.env.PORT) || 3000;

const app = createApp();

const server = app.listen(PORT, () => {
  log("api_started", { port: PORT });
  log("websocket_started", { port: PORT, path: WEBSOCKET_PATH });
});

attachJobEventsWebSocket(server);
