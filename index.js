/**
 * DSH 桥接 · 酒馆扩展端
 * ----------------------------------------------------------------------
 * 这是一个**酒馆扩展**（装在酒馆里），不是一个独立服务。
 *
 * 为什么需要配对的中继：
 *   酒馆扩展跑在浏览器里，浏览器不能监听端口，所以 DSH 没法直接连它。
 *   于是：
 *     酒馆扩展（手机/电脑浏览器） --POST/SSE--> 中继（电脑 :17651） <-- DSH
 *   中继是个很小的 Node 进程，只做转发和排队，不碰酒馆数据。
 *
 * 这一端负责「酒馆内部才有」的东西：
 *   - MVU 变量（`Mvu.getMvuData`）
 *   - 实时消息事件（MESSAGE_RECEIVED / GENERATION_STARTED…）
 *   - 其它扩展的设置（`extensionSettings`）
 *   - 当前上下文（角色、聊天、注入状态）
 * 普通 REST 能拿的（角色卡、世界书、聊天记录）走中继的直连通道，不经过这一端。
 *
 * 配置：在扩展设置里填中继地址（默认 http://127.0.0.1:17651）和一个 token。
 * 没配或连不上时，这个扩展什么都不做，不会影响酒馆。
 */

const MODULE = 'st_dsh_bridge';

/** 默认中继地址。 */
const DEFAULT_RELAY = 'http://127.0.0.1:17651';

/** 轮询间隔（毫秒）：中继用长轮询把 DSH 的指令带回来。 */
const POLL_MS = 1200;

/** 心跳间隔。 */
const HEARTBEAT_MS = 15000;

let settings = null;
let pollTimer = 0;
let heartbeatTimer = 0;
let running = false;
let lastError = '';
let lastSeen = 0;

/* ------------------------------ 设置 ------------------------------ */

function ctx() {
    try {
        return globalThis.SillyTavern?.getContext?.() || null;
    } catch {
        return null;
    }
}

function loadSettings() {
    const c = ctx();
    const store = c?.extensionSettings;
    if (!store) return { enabled: false, relayUrl: DEFAULT_RELAY, token: '', allowWrite: false };
    if (!store[MODULE] || typeof store[MODULE] !== 'object') {
        store[MODULE] = {
            enabled: false,
            relayUrl: DEFAULT_RELAY,
            token: '',
            allowWrite: false,
            autoConnect: true,
        };
    }
    const s = store[MODULE];
    s.relayUrl = String(s.relayUrl || DEFAULT_RELAY).replace(/\/+$/, '');
    s.token = String(s.token || '');
    s.enabled = s.enabled === true;
    s.allowWrite = s.allowWrite === true;
    return s;
}

function saveSettings() {
    try {
        ctx()?.saveSettingsDebounced?.();
    } catch { /* ignore */ }
}

/* ------------------------------ 数据采集 ------------------------------ */

/**
 * MVU 变量：拿不到就返回 null，不抛错。
 *
 * 🔴 坑：MVU（MagVarUpdate / 酒馆助手）是用 `Object.defineProperty` 挂到 window 的，
 * 而且可能比本扩展晚加载。所以不能只查一次 `globalThis.Mvu`——要：
 *   1. 多重入口（Mvu / TavernHelper / 酒馆助手的 getVariables）
 *   2. 每次都重新取（不缓存 API 引用）
 *   3. 拿不到时给出「为什么拿不到」的线索，方便排查
 */
function readMvu() {
    const tried = [];

    // 入口 1：Mvu 官方（MagVarUpdate / 酒馆助手导出）
    try {
        const api = globalThis.Mvu;
        if (!api || typeof api.getMvuData !== 'function') {
            tried.push('Mvu: ' + (api ? '没有 getMvuData' : '未加载'));
        } else {
            // 依次试 chat → message(latest) → 显式楼层
            const attempts = [
                { type: 'chat' },
                { type: 'message', message_id: 'latest' },
            ];
            // 补一个「当前楼层号」，有些版本只认数字
            try {
                const c = ctx();
                const id = Array.isArray(c?.chat) ? c.chat.length - 1 : -1;
                if (id >= 0) attempts.push({ type: 'message', message_id: id });
            } catch { /* ignore */ }

            for (const options of attempts) {
                try {
                    const data = api.getMvuData(options);
                    const stat = data?.stat_data;
                    if (stat && typeof stat === 'object' && Object.keys(stat).length) {
                        return { source: 'Mvu:' + options.type + (options.message_id !== undefined ? '(' + options.message_id + ')' : ''), data: stat };
                    }
                    tried.push('Mvu:' + options.type + ' 返回空');
                } catch (error) {
                    tried.push('Mvu:' + options.type + ' 抛错: ' + String(error?.message || error).slice(0, 60));
                }
            }
        }
    } catch (error) {
        tried.push('Mvu 访问失败: ' + String(error?.message || error).slice(0, 60));
    }

    // 入口 2：酒馆助手 TavernHelper
    try {
        const helper = globalThis.TavernHelper;
        if (helper && typeof helper.getVariables === 'function') {
            const data = helper.getVariables({ type: 'chat' });
            if (data && typeof data === 'object' && Object.keys(data).length) {
                const stat = data.stat_data && typeof data.stat_data === 'object' ? data.stat_data : data;
                if (Object.keys(stat).length) return { source: 'TavernHelper', data: stat };
            }
            tried.push('TavernHelper 返回空');
        } else {
            tried.push('TavernHelper: 未加载');
        }
    } catch (error) {
        tried.push('TavernHelper 抛错: ' + String(error?.message || error).slice(0, 60));
    }

    // 入口 3：直接读聊天元数据
    try {
        const c = ctx();
        const meta = c?.chatMetadata || {};
        for (const key of ['variables', 'MVU', 'mvu', 'stat_data']) {
            const value = meta[key];
            if (!value || typeof value !== 'object') continue;
            const stat = value.stat_data && typeof value.stat_data === 'object' ? value.stat_data : value;
            if (Object.keys(stat).length) return { source: 'chatMetadata.' + key, data: stat };
        }
        tried.push('chatMetadata 里没有 variables/MVU');
    } catch (error) {
        tried.push('chatMetadata 读取失败');
    }

    return { error: '读不到 MVU 变量', tried };
}

/** 当前上下文摘要。 */
function snapshot() {
    const c = ctx();
    if (!c) return { ok: false, reason: '还没有连上酒馆' };
    const character = c.character || {};

    // 世界书绑定：酒馆不同版本放在不同地方，逐个试
    //   1. character.data.extensions.world（卡里声明）
    //   2. character.extensions.world
    //   3. chatMetadata.world_info（聊天级覆盖，可能是字符串或 {global/character/chat} 对象）
    const worldNames = new Set();
    const collect = value => {
        if (!value) return;
        if (typeof value === 'string') { if (value.trim()) worldNames.add(value.trim()); return; }
        if (typeof value === 'object') {
            for (const item of Object.values(value)) {
                if (Array.isArray(item)) for (const name of item) { if (name) worldNames.add(String(name)); }
                else if (typeof item === 'string' && item.trim()) worldNames.add(item.trim());
            }
        }
    };
    collect(character.data?.extensions?.world);
    collect(character.extensions?.world);
    collect(c.chatMetadata?.world_info);
    collect(c.chatMetadata?.worldInfo);

    return {
        ok: true,
        at: Date.now(),
        version: {
            st: String(globalThis.SillyTavern?.version || ''),
            mvu: !!globalThis.Mvu,
        },
        chat: {
            id: String(c.chatId || c.getCurrentChatId?.() || ''),
            length: Array.isArray(c.chat) ? c.chat.length : 0,
            characterName: String(c.name2 || character.name || ''),
            userName: String(c.name1 || ''),
            isGroup: !!c.groupId,
        },
        character: {
            name: String(character.name || c.name2 || ''),
            avatar: String(character.avatar || ''),
            world: [...worldNames].join('、'),
        },
        mvu: readMvu(),
        // 只报 key，不报内容（内容可能有密钥）
        extensionKeys: (() => {
            try {
                return Object.keys(c.extensionSettings || {});
            } catch { return []; }
        })(),
    };
}

/** 最近 N 条消息（含隐藏楼层）。 */
function recentMessages(limit = 30) {
    const c = ctx();
    const chat = Array.isArray(c?.chat) ? c.chat : [];
    return chat.slice(-Math.max(1, Math.min(200, limit))).map((msg, index) => ({
        index: chat.length - Math.min(limit, chat.length) + index,
        isUser: !!msg?.is_user,
        name: String(msg?.name || ''),
        text: String(msg?.mes || ''),
        hidden: !!(msg?.is_system || msg?.is_hidden),
        at: Number(msg?.send_date) || 0,
    }));
}

/**
 * 全局视图：所有角色卡 + 当前聊天之外的信息。
 * 这是「看全局」的关键——不只是当前打开的那一张卡。
 */
function globalOverview() {
    const c = ctx();
    if (!c) return { ok: false, reason: '还没有连上酒馆' };
    const characters = Array.isArray(c.characters) ? c.characters : [];
    const groups = Array.isArray(c.groups) ? c.groups : [];
    const currentName = String(c.name2 || c.character?.name || '');

    return {
        ok: true,
        at: Date.now(),
        current: currentName,
        characters: characters
            .filter(item => item && item.name && item.name !== '{{user}}')
            .map(item => {
                const data = item.data || item;
                const book = data.character_book || data.data?.character_book;
                const worldNames = new Set();
                const collect = value => {
                    if (!value) return;
                    if (typeof value === 'string') { if (value.trim()) worldNames.add(value.trim()); return; }
                };
                collect(data.extensions?.world);
                collect(item.extensions?.world);
                return {
                    name: String(item.name || ''),
                    avatar: String(item.avatar || ''),
                    isCurrent: String(item.name || '') === currentName,
                    entries: Array.isArray(book?.entries) ? book.entries.length : 0,
                    world: [...worldNames].join('、'),
                    tags: Array.isArray(data.tags) ? data.tags.slice(0, 6) : [],
                    creator: String(data.creator || ''),
                    createDate: String(data.create_date || ''),
                };
            }),
        groups: groups.map(item => ({
            name: String(item.name || ''),
            members: Array.isArray(item.members) ? item.members.length : 0,
            id: String(item.id || ''),
        })),
        counts: {
            characters: characters.filter(item => item && item.name && item.name !== '{{user}}').length,
            groups: groups.length,
            extensionSettings: Object.keys(c.extensionSettings || {}).length,
        },
    };
}

/* ------------------------------ 与中继通信 ------------------------------ */

function headers() {
    const h = { 'Content-Type': 'application/json' };
    if (settings?.token) h['X-Bridge-Token'] = settings.token;
    return h;
}

/** 上报一次心跳 + 快照。 */
async function heartbeat() {
    if (!settings?.enabled) return;
    try {
        const response = await fetch(settings.relayUrl + '/api/hi', {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify(snapshot()),
        });
        if (!response.ok) {
            lastError = '中继返回 ' + response.status;
            return;
        }
        lastError = '';
        lastSeen = Date.now();
    } catch (error) {
        lastError = String(error?.message || error);
    }
    renderStatus();
}

/**
 * 长轮询：中继会把 DSH 发来的指令排队，这里取走并执行。
 * 用长轮询而不是 WebSocket，是因为浏览器到局域网 http 的 WS 会被混合内容策略挡掉。
 */
async function poll() {
    if (!settings?.enabled || !running) return;
    try {
        const response = await fetch(settings.relayUrl + '/api/poll', {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({ since: lastSeen }),
            // 中继会挂住最多 25 秒，有指令立刻返回
        });
        if (response.ok) {
            const payload = await response.json();
            for (const job of payload?.jobs || []) {
                await runJob(job);
            }
        }
    } catch { /* 网络抖动忽略，下一轮再试 */ }
    if (running) {
        pollTimer = setTimeout(poll, POLL_MS);
    }
}

/** 执行一条指令，把结果回传。 */
async function runJob(job) {
    const id = String(job?.id || '');
    const kind = String(job?.kind || '');
    let result;
    try {
        result = await handleJob(kind, job?.args || {});
    } catch (error) {
        result = { error: String(error?.message || error) };
    }
    try {
        await fetch(settings.relayUrl + '/api/result', {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({ id, result }),
        });
    } catch { /* ignore */ }
}

/** 指令分派。 */
async function handleJob(kind, args) {
    switch (kind) {
        case 'snapshot':
            return snapshot();
        case 'global':
            return globalOverview();
        case 'messages':
            return { messages: recentMessages(Number(args.limit) || 30) };
        case 'mvu':
            return readMvu();
        case 'switch_character': {
            // 切换角色卡（按名字）。切完让中继那边稍等一下再取快照。
            if (!settings.allowWrite) return { error: '写操作被关闭（在扩展设置里打开 allowWrite）' };
            const name = String(args.name || '').trim();
            if (!name) return { error: '缺少 name' };
            const c = ctx();
            const list = Array.isArray(c?.characters) ? c.characters : [];
            const index = list.findIndex(item => String(item?.name || '') === name);
            if (index < 0) return { error: '没有名为「' + name + '」的角色卡' };
            if (typeof c.selectCharacterById !== 'function') return { error: '这个酒馆版本不支持 selectCharacterById' };
            await c.selectCharacterById(index);
            return { ok: true, switched: name };
        }
        case 'send_message': {
            // 往正文发一条用户消息（会触发 AI 回复）
            if (!settings.allowWrite) return { error: '写操作被关闭（在扩展设置里打开 allowWrite）' };
            const c = ctx();
            if (typeof c?.sendMessageAsUser !== 'function') return { error: '这个酒馆版本不支持 sendMessageAsUser' };
            const text = String(args.text || '').trim();
            if (!text) return { error: '缺少 text' };
            await c.sendMessageAsUser(text);
            return { ok: true, sent: text.length };
        }
        case 'generate': {
            // 后台生成（不写正文）
            const c = ctx();
            const prompt = String(args.prompt || '').trim();
            if (!prompt) return { error: '缺少 prompt' };
            if (typeof c?.generateRaw === 'function') {
                const text = await c.generateRaw({
                    prompt,
                    systemPrompt: String(args.system || ''),
                    responseLength: Math.max(64, Number(args.length) || 600),
                    trimNames: false,
                });
                return { text: String(text || '') };
            }
            if (typeof c?.generateQuietPrompt === 'function') {
                const text = await c.generateQuietPrompt({
                    quietPrompt: [args.system, prompt].filter(Boolean).join('\n\n'),
                    quietToLoud: false,
                    responseLength: Math.max(64, Number(args.length) || 600),
                });
                return { text: String(text || '') };
            }
            return { error: '这个酒馆版本不支持后台生成' };
        }
        case 'extension_settings': {
            // 读某个扩展的设置（只读）
            const key = String(args.key || '').trim();
            if (!key) return { error: '缺少 key' };
            const all = ctx()?.extensionSettings || {};
            return { key, value: all[key] ?? null };
        }
        case 'ping':
            return { ok: true, at: Date.now() };
        default:
            return { error: '不认识的指令：' + kind };
    }
}

/* ------------------------------ 连接控制 ------------------------------ */

function start() {
    if (running) return;
    settings = loadSettings();
    if (!settings.enabled) {
        renderStatus();
        return;
    }
    running = true;
    heartbeat();
    heartbeatTimer = setInterval(heartbeat, HEARTBEAT_MS);
    poll();
    renderStatus();
}

function stop() {
    running = false;
    clearTimeout(pollTimer);
    clearInterval(heartbeatTimer);
    pollTimer = 0;
    heartbeatTimer = 0;
    renderStatus();
}

/* ------------------------------ 设置面板 ------------------------------ */

let panelEl = null;

function buildPanel() {
    const host = document.getElementById('extensions_settings2') || document.getElementById('extensions_settings');
    if (!host) {
        setTimeout(buildPanel, 500);
        return;
    }
    document.getElementById('st_dsh_bridge_panel')?.remove();

    const root = document.createElement('div');
    root.className = 'inline-drawer';
    root.id = 'st_dsh_bridge_panel';

    const head = document.createElement('div');
    head.className = 'inline-drawer-toggle inline-drawer-header';
    const title = document.createElement('b');
    title.textContent = 'DSH 桥接';
    const chevron = document.createElement('div');
    chevron.className = 'inline-drawer-icon fa-solid fa-circle-chevron-down down';
    head.append(title, chevron);

    const content = document.createElement('div');
    content.className = 'inline-drawer-content';

    const hint = document.createElement('div');
    hint.style.cssText = 'font-size:12px;opacity:.75;margin-bottom:8px;line-height:1.5';
    hint.textContent = '让 DSH（电脑上的 AI）读到你酒馆里的 MVU 变量、实时消息和其它扩展设置。需要先在电脑上跑中继（relay/ 目录），填它打印的地址和 token。';
    content.appendChild(hint);

    // 启用
    const enabledRow = checkRow('启用桥接', () => {
        settings.enabled = !settings.enabled;
        saveSettings();
        if (settings.enabled) start(); else stop();
        renderStatus();
    });
    content.appendChild(enabledRow);

    // 中继地址
    content.appendChild(fieldRow('中继地址', 'text', () => settings.relayUrl, value => {
        settings.relayUrl = value.replace(/\/+$/, '');
        saveSettings();
    }, DEFAULT_RELAY));

    // token
    content.appendChild(fieldRow('Token', 'password', () => settings.token, value => {
        settings.token = value;
        saveSettings();
    }, '中继启动时打印的那串'));

    // 允许写
    const writeRow = document.createElement('label');
    writeRow.style.cssText = 'display:flex;align-items:center;gap:8px;padding:6px 0;font-size:13px;cursor:pointer';
    const writeInput = document.createElement('input');
    writeInput.type = 'checkbox';
    writeInput.checked = settings.allowWrite === true;
    writeInput.addEventListener('change', () => {
        settings.allowWrite = writeInput.checked;
        saveSettings();
    });
    const writeSpan = document.createElement('span');
    writeSpan.textContent = '允许 DSH 往正文发消息（默认关）';
    writeRow.append(writeInput, writeSpan);
    content.appendChild(writeRow);

    // 状态
    const status = document.createElement('div');
    status.id = 'st_dsh_bridge_status';
    status.style.cssText = 'font-size:12px;margin-top:8px;padding:8px;border-radius:6px;background:rgba(127,127,127,.1);line-height:1.5;white-space:pre-wrap';
    content.appendChild(status);

    root.append(head, content);
    host.appendChild(root);
    panelEl = root;

    // 折叠交给酒馆自己处理：它对 `.inline-drawer-toggle` 有全局事件委托
    // （script.js 里 `$(document).on('click', '.inline-drawer-toggle', …)`）。
    // 这里再绑一次会导致「展开后立刻收起」——两个 handler 同时 toggle。

    renderStatus();
}

function checkRow(label, onChange) {
    const row = document.createElement('label');
    row.style.cssText = 'display:flex;align-items:center;gap:8px;padding:6px 0;font-size:13px;cursor:pointer';
    const input = document.createElement('input');
    input.type = 'checkbox';
    input.checked = settings?.enabled === true;
    input.addEventListener('change', () => onChange());
    const span = document.createElement('span');
    span.textContent = label;
    row.append(input, span);
    return row;
}

function fieldRow(label, type, getter, setter, placeholder) {
    const row = document.createElement('div');
    row.style.cssText = 'display:flex;flex-direction:column;gap:4px;padding:6px 0;font-size:13px';
    const name = document.createElement('span');
    name.textContent = label;
    const input = document.createElement('input');
    input.type = type;
    input.className = 'text_pole';
    input.value = getter() || '';
    input.placeholder = placeholder || '';
    input.addEventListener('change', () => setter(input.value.trim()));
    row.append(name, input);
    return row;
}

function renderStatus() {
    const el = document.getElementById('st_dsh_bridge_status');
    if (!el) return;
    if (!settings?.enabled) {
        el.textContent = '状态：未启用';
        return;
    }
    if (lastError) {
        el.textContent = '状态：连不上中继\n' + lastError + '\n\n检查：中继是否在跑、地址对不对、token 一致吗。';
        return;
    }
    if (!lastSeen) {
        el.textContent = '状态：正在连接…';
        return;
    }
    const ago = Math.round((Date.now() - lastSeen) / 1000);
    el.textContent = '状态：已连接\n上次通信：' + (ago < 2 ? '刚刚' : ago + ' 秒前') + '\n中继：' + settings.relayUrl;
}

/* ------------------------------ 启动 ------------------------------ */

function boot() {
    settings = loadSettings();
    buildPanel();
    if (settings.enabled && settings.autoConnect !== false) {
        setTimeout(start, 1200);
    }
    // 切聊天时补一次心跳，让中继知道当前是哪张卡
    for (const event of ['CHAT_CHANGED', 'CHARACTER_CHANGED']) {
        try {
            const c = ctx();
            c?.eventSource?.on?.(c.eventTypes?.[event] || event, () => {
                if (running) setTimeout(heartbeat, 600);
            });
        } catch { /* ignore */ }
    }
    // 收到 AI 消息时立刻上报一次，让 DSH 能实时看到
    try {
        const c = ctx();
        c?.eventSource?.on?.(c.eventTypes?.MESSAGE_RECEIVED || 'MESSAGE_RECEIVED', () => {
            if (running) setTimeout(heartbeat, 300);
        });
    } catch { /* ignore */ }
    console.info('[DSH 桥接] 扩展已加载');
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
} else {
    boot();
}

// 酒馆初始化完成后再补一次（设置面板可能还没挂好）
try {
    const c = ctx();
    c?.eventSource?.on?.(c.eventTypes?.APP_READY || 'APP_READY', () => {
        if (!panelEl) buildPanel();
    });
} catch { /* ignore */ }

globalThis.DshBridge = {
    start,
    stop,
    snapshot,
    get status() {
        return { running, lastError, lastSeen, settings };
    },
};
