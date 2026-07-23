const express = require("express");
const http = require("http");
const cors = require("cors");
const WebSocket = require("ws");

const app = express();
app.use(cors({ origin: "*", methods: ["GET", "POST"] }));

const server = http.createServer(app);
const wss = new WebSocket.Server({
    server,
    clientTracking: true,
    perMessageDeflate: false,
    maxPayload: 20 * 1024 * 1024 // 20MB is plenty for a single frame/chunk; lower = less latency risk
});

let sender = null;
const viewers = new Set();

// Tunable: max bytes allowed to sit in a viewer's outgoing buffer before we
// consider them "behind" and start dropping frames for them specifically.
const MAX_VIEWER_BUFFER = 256 * 1024; // 256KB — keep this small for low latency
const MAX_SENDER_BUFFER = 64 * 1024;

app.get("/", (_, res) => res.send("Screen Share Server v4"));

function heartbeat() {
    this.isAlive = true;
}

wss.on("connection", (ws) => {
    ws.binaryType = "arraybuffer";
    ws.role = null;
    ws.isAlive = true;
    ws.on("pong", heartbeat);

    ws.on("message", (message, isBinary) => {
        if (!ws.role) {
            const role = message.toString();
            if (role === "sender") {
                // Only allow one sender; kick the old one so a stale
                // connection doesn't linger and hold the "sender" slot.
                if (sender && sender !== ws && sender.readyState === WebSocket.OPEN) {
                    sender.close(4000, "Replaced by new sender");
                }
                sender = ws;
                ws.role = "sender";
                console.log("Sender connected");
            } else if (role === "viewer") {
                ws.role = "viewer";
                viewers.add(ws);
                console.log("Viewer connected. Total:", viewers.size);
            }
            return;
        }

        if (ws.role === "sender") {
            if (!isBinary) return;

            for (const client of viewers) {
                if (client.readyState !== WebSocket.OPEN) continue;

                // Drop-frame policy: never let a slow viewer queue frames.
                // This is the single biggest latency killer — always send
                // the newest frame, never queue stale ones.
                if (client.bufferedAmount > MAX_VIEWER_BUFFER) continue;

                client.send(message, { binary: true, compress: false });
            }
            return;
        }

        if (ws.role === "viewer") {
            // e.g. control messages from viewer back to sender
            // (resolution requests, "give me a keyframe", etc.)
            if (
                sender &&
                sender.readyState === WebSocket.OPEN &&
                sender.bufferedAmount < MAX_SENDER_BUFFER
            ) {
                sender.send(message);
            }
        }
    });

    ws.on("close", () => {
        if (ws.role === "sender" && sender === ws) sender = null;
        viewers.delete(ws);
    });

    ws.on("error", () => {});
});

// Kill dead sockets so a frozen client doesn't hold a "slow" slot forever.
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 15000);

wss.on("close", () => clearInterval(interval));

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log("Running on", PORT));
