/**
 * DSH 桥接 · 中继
 * ----------------------------------------------------------------------
 * 这个进程跑在**电脑**上（酒馆可能在手机上）。
 *
 * 为什么需要它：
 *   酒馆扩展跑在浏览器里，浏览器不能监听端口，DSH 没法直接连。
 *   中继提供两个通道：
 *     1. 浏览器通道（酒馆扩展 → 中继）：心跳上报快照、长轮询取指令、回传结果
 *     2. DSH 通道（DSH → 中继）：查状态、发指令、直连酒馆 REST
 *
 * 零依赖，只用 node 内置模块。
 *
 * 用法：
 *   node relay.js                    # 默认 :17651
 *   node relay.js --port 17651 --token 你的token
 *   node relay.js --tavern http://192.168.1.20:8000   # 顺便直连酒馆 REST
 *
 * 启动后会打印：
 *   - 本机地址（给酒馆扩展填）
 *   - token（给酒馆扩展填）
 *   - 局域网地址（手机上的酒馆用这个）
 */
import http from 'node:http';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { networkInterfaces } from 'node:os';
import { parseArgs } from 'node:util';

/* ------------------------------ 参数 ------------------------------ */

const { values: argv } = parseArgs({
    options: {
        port: { type: 'string', default: '17651' },
        token: { type: 'string' },
        tavern: { type: 'string' },
        host: { type: 'string', default: '0.0.0.0' },
    },
});

const PORT = Number(argv.port) || 17651;
const TOKEN = String(argv.token || randomUUID().replace(/-/g, '').slice(0, 24));
const TAVERN = argv.tavern ? String(argv.tavern).replace(/\/+$/, '') : '';

/* ------------------------------ 状态 ------------------------------ */

/** 最近一次来自酒馆扩展的快照。 */
let latestSnapshot = null;
/** 酒馆扩展最后通信时间。 */
let lastSeen = 0;

/** 等待酒馆扩展执行的指令：id → { resolve, reject, timer } */
const pending = new Map();
/** 排队中的指令（酒馆扩展下次 poll 时取走）。 */
const queue = [];

/** 长轮询的挂起请求（有指令时立刻放行）。 */
let pollWaiter = null;

/* ------------------------------ 工具 ------------------------------ */

function log(...args) {
    const time = new Date().toLocaleTimeString('zh-CN', { hour12: false });
    console.log('[' + time + ']', ...args);
}

/** 常量时间比较 token。 */
function tokenOk(provided) {
    const a = Buffer.from(String(provided || ''), 'utf8');
    const b = Buffer.from(TOKEN, 'utf8');
    if (a.length !== b.length) return false;
    try {
        return timingSafeEqual(a, b);
    } catch {
        return false;
    }
}

function lanAddresses() {
    const out = [];
    for (const [name, list] of Object.entries(networkInterfaces())) {
        for (const item of list || []) {
            if (item.family !== 'IPv4' || item.internal) continue;
            out.push({ name, address: item.address });
        }
    }
    return out;
}

function readBody(request, limit = 4 * 1024 * 1024) {
    return new Promise((resolve, reject) => {
        let size = 0;
        const chunks = [];
        request.on('data', chunk => {
            size += chunk.length;
            if (size > limit) {
                reject(new Error('请求体太大'));
                request.destroy();
                return;
            }
            chunks.push(chunk);
        });
        request.on('end', () => {
            const text = Buffer.concat(chunks).toString('utf8');
            if (!text) return resolve(null);
            try {
                resolve(JSON.parse(text));
            } catch {
                resolve(null);
            }
        });
        request.on('error', reject);
    });
}

function send(response, status, payload) {
    const body = typeof payload === 'string' ? payload : JSON.stringify(payload);
    response.writeHead(status, {
        'Content-Type': typeof payload === 'string' ? 'text/plain; charset=utf-8' : 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-Bridge-Token',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Cache-Control': 'no-store',
    });
    response.end(body);
}

/** 把指令塞进队列并唤醒 poll。 */
function enqueue(kind, args, timeoutMs = 60000) {
    return new Promise((resolve, reject) => {
        const id = randomUUID();
        const timer = setTimeout(() => {
            pending.delete(id);
            const index = queue.findIndex(job => job.id === id);
            if (index >= 0) queue.splice(index, 1);
            reject(new Error('酒馆扩展没有响应（' + Math.round(timeoutMs / 1000) + ' 秒超时）：确认扩展已启用、地址和 token 对'));
        }, timeoutMs);
        pending.set(id, { resolve, reject, timer });
        queue.push({ id, kind, args });
        if (pollWaiter) {
            const waiter = pollWaiter;
            pollWaiter = null;
            waiter();
        }
    });
}

/* ------------------------------ 路由 ------------------------------ */

const server = http.createServer(async (request, response) => {
    const url = new URL(request.url, 'http://localhost');
    const path = url.pathname;

    if (request.method === 'OPTIONS') return send(response, 204, '');

    /* ---------- 酒馆扩展通道（要 token） ---------- */

    if (path === '/api/hi') {
        if (!tokenOk(request.headers['x-bridge-token'])) return send(response, 401, { error: 'token 不对' });
        const body = await readBody(request);
        if (body) {
            latestSnapshot = { ...body, receivedAt: Date.now() };
            lastSeen = Date.now();
        }
        return send(response, 200, { ok: true });
    }

    if (path === '/api/poll') {
        if (!tokenOk(request.headers['x-bridge-token'])) return send(response, 401, { error: 'token 不对' });
        await readBody(request);
        lastSeen = Date.now();
        // 有排队指令就立刻给，否则挂起最多 25 秒
        if (queue.length) {
            const jobs = queue.splice(0, 8);
            return send(response, 200, { jobs });
        }
        let done = false;
        const finish = () => {
            if (done) return;
            done = true;
            const jobs = queue.splice(0, 8);
            send(response, 200, { jobs });
        };
        pollWaiter = finish;
        setTimeout(finish, 25000);
        return undefined;
    }

    if (path === '/api/result') {
        if (!tokenOk(request.headers['x-bridge-token'])) return send(response, 401, { error: 'token 不对' });
        const body = await readBody(request);
        const id = String(body?.id || '');
        const entry = pending.get(id);
        if (!entry) return send(response, 404, { error: '没有这条指令（可能已超时）' });
        clearTimeout(entry.timer);
        pending.delete(id);
        entry.resolve(body?.result ?? null);
        return send(response, 200, { ok: true });
    }

    /* ---------- DSH 通道（也走 token，防局域网里别人乱用） ---------- */

    if (path === '/dsh/status') {
        if (!tokenOk(url.searchParams.get('token') || request.headers['x-bridge-token'])) {
            return send(response, 401, { error: 'token 不对' });
        }
        return send(response, 200, {
            ok: true,
            connected: Date.now() - lastSeen < 30000,
            lastSeen,
            queued: queue.length,
            pending: pending.size,
            tavern: TAVERN || null,
            snapshot: latestSnapshot,
        });
    }

    if (path === '/dsh/call' && request.method === 'POST') {
        const body = await readBody(request);
        if (!tokenOk(body?.token || request.headers['x-bridge-token'])) {
            return send(response, 401, { error: 'token 不对' });
        }
        const kind = String(body?.kind || '');
        if (!kind) return send(response, 400, { error: '缺少 kind' });
        if (Date.now() - lastSeen > 30000) {
            return send(response, 503, {
                error: '酒馆扩展没连上：确认酒馆页面开着、扩展已启用、中继地址和 token 对',
            });
        }
        try {
            const result = await enqueue(kind, body?.args || {}, Number(body?.timeoutMs) || 60000);
            return send(response, 200, { ok: true, result });
        } catch (error) {
            return send(response, 504, { error: String(error?.message || error) });
        }
    }

    /* ---------- 直连酒馆 REST（可选） ---------- */

    if (path.startsWith('/tavern/')) {
        if (!tokenOk(url.searchParams.get('token') || request.headers['x-bridge-token'])) {
            return send(response, 401, { error: 'token 不对' });
        }
        if (!TAVERN) {
            return send(response, 501, { error: '中继启动时没有传 --tavern，直连通道不可用。例：node relay.js --tavern http://192.168.1.20:8000' });
        }
        const rest = path.slice('/tavern'.length);
        try {
            const upstream = await fetch(TAVERN + rest, {
                method: request.method,
                headers: { 'Content-Type': 'application/json' },
                body: request.method === 'GET' ? undefined : JSON.stringify(await readBody(request) || {}),
                signal: AbortSignal.timeout(30000),
            });
            const text = await upstream.text();
            return send(response, upstream.status, text);
        } catch (error) {
            return send(response, 502, { error: '连不上酒馆：' + String(error?.message || error) });
        }
    }

    /* ---------- 说明页 ---------- */

    if (path === '/' || path === '/index.html') {
        const lan = lanAddresses();
        const html = `<!doctype html><meta charset="utf-8">
<title>DSH 桥接中继</title>
<style>body{font:14px/1.7 system-ui;max-width:720px;margin:40px auto;padding:0 20px;background:#16181d;color:#e6e6ea}
code{background:#24272e;padding:2px 6px;border-radius:4px;color:#7fd6a8}
table{border-collapse:collapse;margin:12px 0}td{padding:6px 14px 6px 0;border-bottom:1px solid #2a2d34}
h2{margin-top:28px;font-size:15px;color:#9aa0ab}</style>
<h1>DSH 桥接中继</h1>
<p>状态：<b style="color:${Date.now() - lastSeen < 30000 ? '#4ade80' : '#f87171'}">${Date.now() - lastSeen < 30000 ? '酒馆已连接' : '酒馆未连接'}</b></p>
<h2>填到酒馆扩展里</h2>
<table>
<tr><td>中继地址</td><td><code>${lan[0] ? 'http://' + lan[0].address + ':' + PORT : 'http://127.0.0.1:' + PORT}</code></td></tr>
<tr><td>Token</td><td><code>${TOKEN}</code></td></tr>
</table>
<p style="opacity:.6">酒馆在手机上时，用上面那个局域网地址；酒馆和这个中继在同一台电脑时，用 <code>http://127.0.0.1:${PORT}</code>。</p>
<h2>可用地址</h2>
<table>
${lan.map(item => '<tr><td>' + item.name + '</td><td><code>http://' + item.address + ':' + PORT + '</code></td></tr>').join('')}
</table>
<h2>DSH 侧</h2>
<p><code>GET /dsh/status?token=...</code> 查状态　<code>POST /dsh/call</code> 发指令</p>
${TAVERN ? '<p>直连酒馆：<code>/tavern/api/...</code> → ' + TAVERN + '</p>' : '<p style="opacity:.6">没传 <code>--tavern</code>，直连通道关闭</p>'}
`;
        response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        return response.end(html);
    }

    return send(response, 404, { error: '没有这个地址：' + path });
});

server.listen(PORT, argv.host, () => {
    const lan = lanAddresses();
    console.log('');
    console.log('  DSH 桥接中继已启动');
    console.log('  ─────────────────────────────────────────');
    console.log('  端口      ' + PORT);
    console.log('  Token     ' + TOKEN);
    console.log('');
    console.log('  填到酒馆扩展里（酒馆在手机上用局域网地址）：');
    for (const item of lan) {
        console.log('    ' + item.name.padEnd(12) + ' http://' + item.address + ':' + PORT);
    }
    console.log('    ' + '本机'.padEnd(12) + ' http://127.0.0.1:' + PORT);
    console.log('');
    if (TAVERN) console.log('  直连酒馆  ' + TAVERN);
    else console.log('  直连酒馆  未开启（加 --tavern http://手机IP:8000）');
    console.log('  说明页    http://127.0.0.1:' + PORT + '/');
    console.log('');
});

for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => {
        log('收到 ' + signal + '，退出');
        server.close(() => process.exit(0));
        setTimeout(() => process.exit(0), 1000);
    });
}
