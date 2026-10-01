import "dotenv/config";
import { createApp } from "./api/app";
import { attachJobEventsWebSocket, WEBSOCKET_PATH } from "./api/websocket";

const PORT = Number(process.env.PORT) || 3000;

const app = createApp();

const server = app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
  console.log(`Job events WebSocket at ws://localhost:${PORT}${WEBSOCKET_PATH}`);
});

attachJobEventsWebSocket(server);
