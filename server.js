const express = require("express");
const http = require("http");
const cors = require("cors");
const WebSocket = require("ws");

const app = express();

app.use(cors({
    origin: "*",
    methods: ["GET", "POST"]
}));

const server = http.createServer(app);

const wss = new WebSocket.Server({
    server,
    clientTracking: true,
    perMessageDeflate: false,
    maxPayload: 100 * 1024 * 1024
});

let sender = null;
const viewers = new Set();

app.get("/", (_, res) => {
    res.send("Screen Share Server v3");
});

wss.on("connection", (ws) => {

    ws.binaryType = "arraybuffer";
    ws.role = null;

    ws.on("message", (message, isBinary) => {

        if (!ws.role) {

            const role = message.toString();

            if (role === "sender") {
                sender = ws;
                ws.role = "sender";
                console.log("Sender Connected");
            }

            if (role === "viewer") {
                ws.role = "viewer";
                viewers.add(ws);
                console.log("Viewer Connected");
            }

            return;
        }

        if (ws.role === "sender") {

            if (!isBinary) return;

            for (const client of viewers) {

                if (client.readyState !== WebSocket.OPEN)
                    continue;

                // Skip clients that are already behind.
                if (client.bufferedAmount > 0)
                    continue;

                client.send(message, {
                    binary: true,
                    compress: false
                });
            }

            return;
        }

        if (ws.role === "viewer") {

            if (
                sender &&
                sender.readyState === WebSocket.OPEN &&
                sender.bufferedAmount < 65536
            ) {
                sender.send(message);
            }
        }

    });

    ws.on("close", () => {

        if (ws.role === "sender")
            sender = null;

        viewers.delete(ws);
    });

    ws.on("error", () => {});
});

const PORT = process.env.PORT || 8080;

server.listen(PORT, () => {
    console.log("Running on", PORT);
});
