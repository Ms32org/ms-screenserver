const express = require('express');
const http = require('http');
const cors = require('cors');
const WebSocket = require('ws');

const app = express();
app.use(cors());

const server = http.createServer(app);

const wss = new WebSocket.Server({
    server,
    clientTracking: true,
    perMessageDeflate: false,
});

let sender = null;
let viewers = new Set();

app.get('/', (req, res) => {
    res.send('screen share server v2');
});

wss.on('connection', (ws) => {
    console.log('[CONNECT] New WebSocket client');

    ws.role = null;

    ws.on('message', (message, isBinary) => {
        try {
            // ===== ROLE IDENTIFICATION =====
            if (!ws.role) {
                const msg = message.toString();

                if (msg === "sender") {
                    sender = ws;
                    ws.role = "sender";
                    console.log("[ROLE] Sender connected");
                } 
                else if (msg === "viewer") {
                    ws.role = "viewer";
                    viewers.add(ws);
                    console.log("[ROLE] Viewer connected");
                }

                return;
            }

            // ===== SENDER LOGIC =====
            if (ws.role === "sender") {
                if (isBinary) {
                    // Forward screen frames to all viewers
                    viewers.forEach((client) => {
                        if (client.readyState === WebSocket.OPEN) {
                            client.send(message, { binary: true });
                        }
                    });
                } else {
                    console.log("[WARN] Sender sent non-binary");
                }
            }

            // ===== VIEWER LOGIC =====
            else if (ws.role === "viewer") {
                // Forward controls to sender
                if (sender && sender.readyState === WebSocket.OPEN) {
                    sender.send(message.toString());
                }
            }

        } catch (err) {
            console.error("[ERROR]", err);
        }
    });

    ws.on('close', () => {
        console.log('[DISCONNECT] Client');

        if (ws.role === "sender") {
            sender = null;
            console.log("[INFO] Sender disconnected");
        }

        if (ws.role === "viewer") {
            viewers.delete(ws);
            console.log("[INFO] Viewer removed");
        }
    });

    ws.on('error', (err) => {
        console.error('[WS ERROR]', err);
    });
});

const PORT = process.env.PORT || 8080;

server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});