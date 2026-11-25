// memory-worker（阶段 3）：独立后台整理进程。
//
// 职责：
//   1. 只读 QQbridge 的 state/social-v2.json，取「上次游标之后」的新消息；
//   2. 达到阈值就建/复用一个独立 DSH 会话，把消息压成 JSON；
//   3. 校验、落盘（时间线 / 成员事实 / 动态人设 / 审计），推进游标；
//   4. 解析失败不推进游标，下一轮重试。
//
// 不复用桥接进程里的定时器，也不 import 桥接的业务代码；
// 只 import 它的**上游客户端**（dsh-client.js / dsh-endpoint.js），
// 那是稳定的 DSH 通信层，读它不会产生任何上游 diff。
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import { loadMemoryConfig } from '../core/config.mjs';
import { anonymizeConversation, appendMetric } from '../core/metrics.mjs';
import { acquireWorkerLock } from '../core/worker-lock.mjs';
import { evaluatePendingTrigger } from '../core/batching.mjs';
import {
  deleteScopedSession,
  getScopedSession,
  nextSessionBudget,
  normalizeConsolidationPool,
  sessionRetirementReason,
  setScopedSession
} from '../core/session-budget.mjs';
import {
  applyConsolidation,
  buildConsolidationPrompt,
  buildConsolidationRepairPrompt,
  parseConsolidationOutput,
  readCursor
} from '../core/consolidate.mjs';
import { validatePatchEnvelope } from '../core/patches.mjs';
import {
  beginPendingBatch,
  commitPendingBatch,
  failPendingBatch,
  pendingBatch,
  scanIntoPending
} from '../core/pending.mjs';
import { listConversations, loadSocialSnapshot, loadSocialV2, readMessages } from '../source/social-v2.mjs';
import { loadQQbridgeDshAdapter } from '../adapters/qqbridge-dsh.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = process.env.MEMORY_ROOT ? path.resolve(process.env.MEMORY_ROOT) : path.resolve(HERE, '..', '..');
const CFG = loadMemoryConfig(REPO);
const DATA = CFG.dataDir;

const LOG_FILE = path.join(DATA, 'logs', 'worker.log');
const SESSION_FILE = path.join(DATA, '.state', 'worker-session.json');
const GAP_FILE = path.join(DATA, '.state', 'gaps.jsonl');

const log = (msg) => {
  const line = `${new Date().toISOString()} ${msg}`;
  console.log(line);
  try {
    fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });
    fs.appendFileSync(LOG_FILE, line + '\n');
  } catch {
    /* 日志写不进去不影响主流程 */
  }
};

const readJson = (file, fallback = null) => {
  try {
    let raw = fs.readFileSync(file, 'utf8');
    if (raw.charCodeAt(0) === 0xfeff) raw = raw.slice(1);
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
};

const writeJson = (file, obj) => {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2) + '\n', 'utf8');
  fs.renameSync(tmp, file);
};

const recordSequenceIssue = (key, details) => {
  const entry = { at: new Date().toISOString(), key, ...details };
  try {
    fs.mkdirSync(path.dirname(GAP_FILE), { recursive: true });
    fs.appendFileSync(GAP_FILE, JSON.stringify(entry) + '\n', 'utf8');
  } catch (error) {
    log(`⚠ 无法记录消息断档 ${key}：${error?.message ?? error}`);
  }
};

const VERSION = readJson(path.join(REPO, 'package.json'), {})?.version ?? 'unknown';
const workerLock = acquireWorkerLock(DATA, { ...CFG.worker.lock, version: VERSION });
if (!workerLock.ok) {
  const holder = workerLock.holder;
  log(`✗ worker 已在运行（pid=${holder?.pid ?? '?'}，启动=${holder?.startedAt ?? '?'}）；本进程不再启动`);
  process.exit(23);
}

// ── 复用桥接的 DSH 客户端（只读 import）─────────────────────────────────────
let NodeApiClient;
let createTurnCollector;
let unwrap;
let discoverDshBaseUrl;
try {
  ({ NodeApiClient, createTurnCollector, unwrap, discoverDshBaseUrl } = await loadQQbridgeDshAdapter(CFG));
} catch (error) {
  log(`✗ 加载 DSH 客户端失败（检查 config.worker.dshClientPath）：${error?.message ?? error}`);
  workerLock.release();
  process.exit(2);
}

let api = null;
let baseUrl = '';

// ── 事件流与 turn 收集 ──────────────────────────────────────────────────────
const collectors = new Map(); // sessionId -> turn collector
const turnUsages = new Map(); // sessionId -> 本轮最后一条 provider usage
const turnSteps = new Map(); // sessionId -> assistant attempt/message 数
const waiters = new Map(); // sessionId -> { resolve, reject, timer }
let streamAbort = null;

function settle(sessionId, kind, value) {
  const w = waiters.get(sessionId);
  if (!w) return;
  waiters.delete(sessionId);
  collectors.delete(sessionId);
  turnUsages.delete(sessionId);
  const steps = Math.max(1, Number(turnSteps.get(sessionId)) || 0);
  turnSteps.delete(sessionId);
  clearTimeout(w.timer);
  if (kind === 'ok') w.resolve({ ...value, steps });
  else w.reject(value instanceof Error ? value : new Error(String(value)));
}

function handleFrame(frame) {
  if (!frame || frame.type !== 'session/event') return;
  const sessionId = frame.sessionId;
  if (!waiters.has(sessionId)) return;
  const event = frame.event;
  // 只在 turn/start 建 collector：迟到的非 turn 事件不会留下永远等不到 end 的空壳。
  let collector = collectors.get(sessionId);
  if (!collector) {
    if (event?.type !== 'turn/start') return;
    collector = createTurnCollector();
    collectors.set(sessionId, collector);
    turnUsages.set(sessionId, null);
    turnSteps.set(sessionId, 0);
  }
  if ((event?.type === 'assistant/message' || event?.type === 'assistant/attempt')) {
    turnSteps.set(sessionId, (turnSteps.get(sessionId) ?? 0) + 1);
    let usage = event?.data?.usage;
    if (usage === undefined && Array.isArray(event?.data?.stream)) {
      for (let i = event.data.stream.length - 1; i >= 0; i -= 1) {
        if (event.data.stream[i]?.usage !== undefined) {
          usage = event.data.stream[i].usage;
          break;
        }
      }
    }
    if (usage && typeof usage === 'object') turnUsages.set(sessionId, usage);
  }
  const ended = collector.push(event);
  if (!ended) return;
  const usage = turnUsages.get(sessionId);
  if (ended.reason?.kind === 'completed' && String(ended.text ?? '').trim()) settle(sessionId, 'ok', { text: ended.text, usage });
  else settle(sessionId, 'err', `turn 未正常完成：${ended.reason?.kind ?? 'unknown'}`);
}

function normalizeUsage(usage) {
  const pick = (...keys) => {
    for (const key of keys) {
      const n = Number(usage?.[key]);
      if (Number.isFinite(n) && n > 0) return Math.round(n);
    }
    return 0;
  };
  return {
    cacheMiss: pick('cacheMiss', 'uncachedInputTokens', 'inputTokens'),
    cacheHit: pick('cacheHit', 'cacheReadTokens'),
    cacheWrite: pick('cacheWrite', 'cacheWriteTokens'),
    output: pick('output', 'outputTokens'),
    reasoning: pick('reasoning', 'reasoningTokens')
  };
}

function mergeUsage(...items) {
  const total = { cacheMiss: 0, cacheHit: 0, cacheWrite: 0, output: 0, reasoning: 0 };
  for (const item of items) {
    for (const key of Object.keys(total)) total[key] += Number(item?.[key]) || 0;
  }
  return total;
}

function patchValidationErrors(parsed, messages, key, gid) {
  if (!parsed?.ok) return [String(parsed?.error ?? '没有找到合法 JSON 对象')];
  if (!parsed.patchEnvelope) return ['必须输出 {"schema":1,"patches":[...]}，不再接受旧 timeline/members 结果格式'];
  const checked = validatePatchEnvelope(parsed.patchEnvelope, { memDir: DATA, key, gid, messages });
  return checked.ok ? [] : checked.errors;
}

async function eventLoop(signal) {
  for await (const envelope of api.events.mux({ signal })) {
    handleFrame(envelope?.payload);
  }
}

function startStream() {
  if (streamAbort) return;
  const controller = new AbortController();
  streamAbort = controller;
  const run = async () => {
    while (!controller.signal.aborted) {
      try {
        await eventLoop(controller.signal);
      } catch (error) {
        if (controller.signal.aborted) return;
        log(`事件流断开：${error?.message ?? error}（5 秒后重连）`);
      }
      if (controller.signal.aborted) return;
      await new Promise((r) => setTimeout(r, 5000));
      try {
        api.invalidateAuth?.();
      } catch {}
    }
  };
  void run().finally(() => {
    if (streamAbort === controller) streamAbort = null;
  });
}

function waitTurn(sessionId, timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => settle(sessionId, 'err', `等待整理 turn 超时（${timeoutMs}ms）`), timeoutMs);
    timer.unref?.();
    waiters.set(sessionId, { resolve, reject, timer });
  });
}

// ── DSH 连接与会话 ──────────────────────────────────────────────────────────
async function connect() {
  const found = await discoverDshBaseUrl({ current: CFG.worker.dshBaseUrl, log: () => {} });
  if (!found) throw new Error('找不到正在运行的 DSH（端口自愈失败）');
  baseUrl = found;
  api = new NodeApiClient(found);
  await api.ensureAuth();
  log(`已连接 DSH：${found}`);
}

function isDshConnectionError(error) {
  return /fetch failed|ECONN(?:REFUSED|RESET)|socket|network|连接|端口|auth/i.test(String(error?.message ?? error));
}

async function reconnectDsh(error) {
  log(`⚠ DSH 连接失效（${error?.message ?? error}），重新发现端口并重建事件流`);
  const oldController = streamAbort;
  streamAbort = null;
  try { oldController?.abort(); } catch {}
  try { api?.invalidateAuth?.(); } catch {}
  await connect();
  startStream();
}

async function archiveRemoteSession(entry, reason = 'evicted') {
  if (!entry?.sessionId) return;
  try { await api?.stopSessionWork?.(entry.sessionId); } catch {}
  try { await api?.workspace?.archiveSession({ sessionId: entry.sessionId }); } catch {}
  log(`♻ 退役整理会话 ${entry.sessionId}（${entry.key || 'unknown'}）：${reason}（batches=${entry.batches ?? 0}, steps=${entry.steps ?? 0}, tokens=${entry.tokens ?? 0}）`);
}

async function ensureSession(key) {
  const preset = CFG.consolidate.preset || 'qq-chat';
  const pool = normalizeConsolidationPool(readJson(SESSION_FILE, null));
  const saved = getScopedSession(pool, { key, preset, baseUrl });
  if (saved?.sessionId) {
    api.events.follow(saved.sessionId);
    return saved.sessionId;
  }
  const dir = path.join(DATA, '.state', 'worker-agent');
  fs.mkdirSync(dir, { recursive: true });
  const ws = unwrap(await api.workspace.create({ path: dir }), 'workspace.create');
  const created = unwrap(
    await api.sessions.create({ workspaceId: ws.workspace.workspaceId, agentPreset: preset }),
    'session.create'
  );
  const sessionId = created.sessionId;
  await api.sessions.selectModel({
    sessionId,
    provider: CFG.worker.provider,
    model: CFG.worker.model,
    reasoningEffort: CFG.worker.reasoningEffort
  });
  const entry = { sessionId, workspaceId: ws.workspace.workspaceId, key, preset, baseUrl, batches: 0, steps: 0, tokens: 0, updatedAt: Date.now() };
  const updated = setScopedSession(pool, key, entry, { maxSessions: 8 });
  writeJson(SESSION_FILE, updated.pool);
  for (const evicted of updated.evicted) await archiveRemoteSession(evicted, '会话池上限淘汰');
  api.events.follow(sessionId);
  log(`已创建隔离整理会话：${sessionId}（key=${key}，preset=${preset}）`);
  return sessionId;
}

async function dropSession(key = '', sessionId = '') {
  if (!key && !sessionId) {
    try { fs.unlinkSync(SESSION_FILE); } catch {}
    return;
  }
  const current = readJson(SESSION_FILE, null);
  if (!current) return;
  const { pool } = deleteScopedSession(current, { key, sessionId });
  writeJson(SESSION_FILE, pool);
}

async function retireConsolidationSession(key, sessionId, reason) {
  const current = readJson(SESSION_FILE, null);
  const { pool, removed } = deleteScopedSession(current, { key, sessionId });
  if (!removed.length) return;
  writeJson(SESSION_FILE, pool);
  for (const entry of removed) await archiveRemoteSession(entry, reason);
}

async function accountConsolidationSession(key, sessionId, turn, usage) {
  const pool = normalizeConsolidationPool(readJson(SESSION_FILE, null));
  const saved = pool.sessions[String(key)];
  if (!saved || saved.sessionId !== sessionId) return '';
  const budget = nextSessionBudget(saved, { steps: turn?.steps, usage });
  const next = { ...saved, ...budget, updatedAt: Date.now() };
  pool.sessions[String(key)] = next;
  writeJson(SESSION_FILE, pool);
  const reason = sessionRetirementReason(next, CFG.worker.consolidationSession);
  if (reason) await retireConsolidationSession(key, sessionId, reason);
  return reason;
}

function scanConversationItem(item, conversationsSnapshot = null) {
  const { key } = item;
  const { messages, error } = conversationsSnapshot
    ? readMessages(conversationsSnapshot, key)
    : readMessages(CFG.bridge.socialV2, key);
  if (error) return { ok: false, error };
  const legacy = readCursor(DATA, key);
  const scanned = scanIntoPending({
    memDir: DATA,
    key,
    messages,
    initialSeq: legacy.lastSeq,
    initialAt: legacy.lastAt
  });
  const { sequenceReset, gap } = scanned;
  if (sequenceReset || gap) {
    const detail = {
      scanSeq: scanned.queue.scan.seq,
      generation: scanned.queue.generation,
      firstSeq: scanned.firstSeq,
      sourceLastSeq: scanned.sourceLastSeq,
      sequenceReset,
      gap
    };
    recordSequenceIssue(key, detail);
    log(
      `⚠ ${key} 消息序号${sequenceReset ? '发生回退/重置' : '存在断档'}：`
      + `generation=${detail.generation}, visible=${detail.firstSeq}-${detail.sourceLastSeq}`
      + `${gap ? `, missing=${gap.from}-${gap.to}` : ''}；可恢复消息已进入 pending`
    );
  }
  return { ok: true, scanned };
}

// ── 一轮整理 ────────────────────────────────────────────────────────────────
async function consolidateConversation(item, conversationsSnapshot = null) {
  const { key, kind, id } = item;
  const gid = kind === 'group' ? id : '';
  const scanRes = scanConversationItem(item, conversationsSnapshot);
  if (!scanRes.ok) return { ok: false, reason: 'source-unavailable', rawMessages: 0 };
  const { scanned } = scanRes;
  const { sequenceReset, gap } = scanned;
  const queued = scanned.queue.pending;
  const decision = evaluatePendingTrigger(scanned.queue, CFG.consolidate);
  if (!decision.ready) {
    return {
      ok: true,
      skipped: `${decision.reason}(pending=${queued.length}, quiet=${Math.round(decision.quietForMs / 60000)}m, wait=${Math.round(decision.waitingForMs / 3_600_000)}h)`,
      reason: decision.reason,
      rawMessages: 0,
      informativeMessages: queued.length,
      pendingDepth: queued.length,
      scannedMessages: scanned.counts.scanned,
      sequenceReset,
      gapDetected: Boolean(gap)
    };
  }

  const promptMaxChars = CFG.consolidate.promptMaxChars;
  const preview = pendingBatch(scanned.queue, CFG.consolidate.maxMessages, { maxChars: promptMaxChars });

  const meta = gid ? readJson(path.join(DATA, 'groups', gid, 'meta.json'), null) : null;
  const groupName = meta?.name ?? key;

  let sessionId;
  try {
    sessionId = await ensureSession(key);
  } catch (error) {
    await dropSession(key);
    return { ok: false, reason: 'session-unavailable', rawMessages: 0, informativeMessages: preview.length, pendingDepth: queued.length, sequenceReset, gapDetected: Boolean(gap) };
  }

  const trigger = decision.trigger;
  const startedBatch = beginPendingBatch({
    memDir: DATA,
    key,
    maxMessages: CFG.consolidate.maxMessages,
    maxChars: promptMaxChars,
    trigger
  });
  const pending = startedBatch.items;
  const batchId = startedBatch.batchId;
  const failBatch = (reason) => failPendingBatch({
    memDir: DATA,
    key,
    batchId,
    reason,
    maxAttempts: CFG.consolidate.maxBatchAttempts
  });
  const prompt = buildConsolidationPrompt({ groupName, key, messages: pending, maxChars: promptMaxChars, memDir: DATA });
  // 先登记 waiter 再发 prompt，避免极快的 turn/start + turn/end 在 waiter 建立前丢失。
  const turnPromise = waitTurn(sessionId, CFG.consolidate.timeoutMs);
  let accepted;
  try {
    accepted = await api.sessions.prompt({
      sessionId,
      mode: 'queue',
      content: [{ type: 'text', text: prompt }]
    });
  } catch (error) {
    settle(sessionId, 'err', error);
    try { await turnPromise; } catch {}
    failBatch('prompt-exception');
    throw error;
  }
  if (!accepted?.result?.ok) {
    const code = String(accepted?.result?.error?.code ?? '?');
    settle(sessionId, 'err', `prompt-rejected:${code}`);
    try { await turnPromise; } catch {}
    if (/session|not.?found|404/i.test(code)) await dropSession(key, sessionId);
    failBatch('prompt-rejected');
    return { ok: false, reason: 'prompt-rejected', rawMessages: pending.length, informativeMessages: pending.length, pendingDepth: queued.length, trigger, sequenceReset, gapDetected: Boolean(gap) };
  }

  let turn;
  try {
    turn = await turnPromise;
  } catch (error) {
    failBatch('turn-failed');
    throw error;
  }
  let output = turn.text;
  let usage = normalizeUsage(turn.usage);
  let parsed = parseConsolidationOutput(output);
  if (!parsed.ok) {
    // 关键：解析失败**不推进游标**，下一轮重试（达上限则入死信隔离）
    log(`✗ ${key} 解析失败（输出 ${String(output).length} 字）：${String(output).replace(/\s+/g, ' ').slice(0, 100)}`);
    failBatch('parse-failed');
    await accountConsolidationSession(key, sessionId, turn, usage);
    return { ok: false, reason: 'parse-failed', rawMessages: pending.length, informativeMessages: pending.length, pendingDepth: queued.length, trigger, usage, sequenceReset, gapDetected: Boolean(gap) };
  }

  let validationErrors = patchValidationErrors(parsed, pending, key, gid);
  if (validationErrors.length) {
    log(`⚠ ${key} 补丁不合规，进行一次纠错：${validationErrors.slice(0, 4).join('；')}`);
    const repairPrompt = buildConsolidationRepairPrompt({ groupName, key, messages: pending, errors: validationErrors, maxChars: promptMaxChars, memDir: DATA });
    const repairPromise = waitTurn(sessionId, CFG.consolidate.timeoutMs);
    let repairAccepted;
    try {
      repairAccepted = await api.sessions.prompt({
        sessionId,
        mode: 'queue',
        content: [{ type: 'text', text: repairPrompt }]
      });
    } catch (error) {
      settle(sessionId, 'err', error);
      try { await repairPromise; } catch {}
      failBatch('repair-prompt-exception');
      throw error;
    }
    if (!repairAccepted?.result?.ok) {
      const code = String(repairAccepted?.result?.error?.code ?? '?');
      settle(sessionId, 'err', `repair-prompt-rejected:${code}`);
      try { await repairPromise; } catch {}
      failBatch('repair-prompt-rejected');
      await accountConsolidationSession(key, sessionId, turn, usage);
      return { ok: false, reason: 'repair-prompt-rejected', rawMessages: pending.length, informativeMessages: pending.length, pendingDepth: queued.length, trigger, usage, sequenceReset, gapDetected: Boolean(gap) };
    }

    let repairTurn;
    try {
      repairTurn = await repairPromise;
    } catch (error) {
      failBatch('repair-turn-failed');
      throw error;
    }
    usage = mergeUsage(usage, normalizeUsage(repairTurn.usage));
    turn = { ...repairTurn, steps: (Number(turn.steps) || 0) + (Number(repairTurn.steps) || 0) };
    output = repairTurn.text;
    parsed = parseConsolidationOutput(output);
    validationErrors = patchValidationErrors(parsed, pending, key, gid);
    if (validationErrors.length) {
      log(`✗ ${key} 补丁纠错仍失败：${validationErrors.slice(0, 8).join('；')}`);
      failBatch('patch-invalid');
      await accountConsolidationSession(key, sessionId, turn, usage);
      await retireConsolidationSession(key, sessionId, '补丁纠错仍失败');
      return { ok: false, reason: 'patch-invalid', rawMessages: pending.length, informativeMessages: pending.length, pendingDepth: queued.length, trigger, usage, sequenceReset, gapDetected: Boolean(gap) };
    }
    log(`✓ ${key} 补丁纠错通过`);
  }
  // 只有确实写了补丁却没有 timeline 时才值得警告；整批空补丁属于模型按约定交白卷。
  const gavePatches = Boolean(parsed.timeline?.length || parsed.members?.length || parsed.persona);
  if (gid && gavePatches && !parsed.timeline?.length) {
    log(`⚠ ${key} 本批有补丁但没有 timeline（输出 ${String(output).length} 字）`);
  }

  let applied;
  try {
    applied = applyConsolidation({ memDir: DATA, key, gid, output: parsed, messages: pending });
    commitPendingBatch({ memDir: DATA, key, batchId, result: applied });
  } catch (error) {
    failBatch('apply-failed');
    throw error;
  }
  const sessionRetiredBy = await accountConsolidationSession(key, sessionId, turn, usage);
  log(
    `✓ ${key}：时间线 +${applied.timelinePoints}，事实 +${applied.memberFacts}，`
    + `活跃度 +${applied.interactions}，人设${applied.personaUpdated ? '已更新' : '未变'}，`
    + `共 ${pending.length} 条，seq→${applied.lastSeq}，审计 ${applied.auditId}`
  );
  return {
    ok: true,
    reason: 'ok',
    ...applied,
    count: pending.length,
    rawMessages: pending.length,
    informativeMessages: pending.length,
    pendingDepth: Math.max(0, queued.length - pending.length),
    trigger,
    usage,
    sessionRetiredBy,
    sequenceReset,
    gapDetected: Boolean(gap)
  };
}

function recordConsolidationMetric(item, result, durationMs) {
  let conversation = 'hmac:unknown';
  try { conversation = anonymizeConversation(DATA, item.key); } catch {}
  const skipped = Boolean(result?.skipped);
  appendMetric(DATA, {
    type: 'consolidate',
    conversation,
    status: skipped ? 'skipped' : result?.ok ? 'success' : 'failed',
    reason: result?.reason ?? 'exception',
    durationMs,
    rawMessages: result?.rawMessages,
    informativeMessages: result?.informativeMessages,
    pendingDepth: result?.pendingDepth,
    scannedMessages: result?.scannedMessages,
    trigger: result?.trigger,
    timelinePoints: result?.timelinePoints,
    memberFacts: result?.memberFacts,
    interactions: result?.interactions,
    dateClamped: result?.dateClamped,
    personaUpdated: result?.personaUpdated,
    sequenceReset: result?.sequenceReset,
    gapDetected: result?.gapDetected,
    sessionRetiredBy: result?.sessionRetiredBy,
    usage: result?.usage
  }, CFG.metrics);
}

// ── 会话回收（省 token）────────────────────────────────────────────────────
// 长命 DSH 会话的上下文会涨到几十万 token，而每一步工具调用都要整段重发。
// 桥接每次唤醒本身会带最近若干条消息，所以闲置时把会话回收掉，聊天连续性几乎不受影响，
// 下一轮唤醒会按新会话重新 bootstrap（顺带把 boot 文案重新注入一次）。
const RESET_FILE = path.join(DATA, '.state', 'resets.json');

function bridgePost(urlPath, body) {
  const token = (() => {
    try {
      return fs.readFileSync(CFG.bridge.consoleTokenFile, 'utf8').trim();
    } catch {
      return '';
    }
  })();
  const payload = JSON.stringify(body ?? {});
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: '127.0.0.1',
        port: CFG.bridge.consolePort,
        path: urlPath,
        method: 'POST',
        timeout: 5000,
        headers: {
          'x-console-token': token,
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(payload)
        }
      },
      (res) => {
        let data = '';
        res.setEncoding('utf8');
        res.on('data', (c) => {
          data += c;
        });
        res.on('end', () => {
          let parsed = data;
          try {
            parsed = JSON.parse(data);
          } catch {
            /* 保留原文 */
          }
          resolve({ status: res.statusCode, body: parsed });
        });
      }
    );
    req.on('timeout', () => req.destroy(new Error('超时')));
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

async function maybeRecycleSessions(conversationsSnapshot = null) {
  const opt = CFG.worker.reset;
  if (!opt?.enabled) return;
  const now = Date.now();
  const state = readJson(RESET_FILE, {}) || {};
  const loaded = conversationsSnapshot
    ? { conversations: conversationsSnapshot, error: '' }
    : loadSocialV2(CFG.bridge.socialV2);
  const { conversations, error } = loaded;
  if (error) {
    log(`⚠ 会话退役跳过：${error}`);
    return;
  }
  const sessions = readJson(path.join(CFG.bridge.home, 'state', 'sessions.json'), {})?.sessions || {};
  let recycled = 0;
  for (const [key, st] of Object.entries(conversations)) {
    if (!/^(group|private):\d{1,20}$/.test(key)) continue;
    if (!sessions[key]) continue; // 没有活跃 DSH 会话，无需回收
    const lastActivity = Math.max(
      Number(st?.lastIncomingAt) || 0,
      Number(st?.lastAiReplyAt) || 0,
      Number(state[key]?.lastResetAt) || 0
    );
    if (now - lastActivity < opt.idleMs) continue;
    if (now - (Number(state[key]?.lastResetAt) || 0) < opt.intervalMs) continue;
    try {
      // retire 只撤销/归档 DSH 会话并让下次重新 bootstrap；必须保留 social-v2
      // 消息与 seq，否则长期记忆游标会被新一代 seq=1 卡死。
      const res = await bridgePost('/api/session/retire', { key });
      if (res.status === 200 && res.body?.ok !== false) {
        state[key] = { lastResetAt: now, at: new Date(now).toISOString() };
        writeJson(RESET_FILE, state);
        recycled += 1;
        log(`♻ 退役 DSH 会话 ${key}：闲置 ${Math.round((now - lastActivity) / 60000)} 分钟（social-v2 已保留）`);
      } else {
        log(`⚠ 回收 ${key} 失败：HTTP ${res.status} ${JSON.stringify(res.body).slice(0, 120)}`);
      }
    } catch (error) {
      log(`⚠ 回收 ${key} 异常：${error?.message ?? error}`);
    }
  }
  if (recycled) log(`本轮回收 ${recycled} 个会话（省 token）`);
}

let taskChain = Promise.resolve();
function runSerialized(fn) {
  const next = taskChain.then(fn, fn);
  taskChain = next.catch(() => {});
  return next;
}

let lastScannedSig = '';
let sourceWatcher = null;
let watchDebounceTimer = null;
let watchFallbackTimer = null;

function targetItems(items) {
  return CFG.consolidate.includePrivate ? items : items.filter((i) => i.kind === 'group');
}

/** 高频纯本地增量扫描（不调模型，零 token 消耗），防止活跃群刷屏滚出 social-v2 窗口造成断档。 */
function scanOnlyIfChanged() {
  return runSerialized(async () => {
    if (!CFG.consolidate.enabled || stopping) return;
    const snap = loadSocialSnapshot(CFG.bridge.socialV2);
    if (snap.error || !snap.sig || snap.sig === lastScannedSig) return;
    lastScannedSig = snap.sig;
    for (const item of targetItems(snap.items)) {
      try {
        scanConversationItem(item, snap.conversations);
      } catch (error) {
        log(`⚠ 快速扫描 ${item.key} 异常：${error?.message ?? error}`);
      }
    }
  });
}

function startSourceWatcher() {
  const targetFile = path.resolve(CFG.bridge.socialV2);
  const dir = path.dirname(targetFile);
  const baseName = path.basename(targetFile);
  const triggerScan = () => {
    if (stopping || watchDebounceTimer) return;
    watchDebounceTimer = setTimeout(() => {
      watchDebounceTimer = null;
      void scanOnlyIfChanged();
    }, 1200);
    watchDebounceTimer.unref?.();
  };
  try {
    if (fs.existsSync(dir)) {
      sourceWatcher = fs.watch(dir, (_event, filename) => {
        if (!filename || String(filename) === baseName) triggerScan();
      });
      sourceWatcher.on('error', () => {});
      sourceWatcher.unref?.();
    }
  } catch {}
  watchFallbackTimer = setInterval(() => {
    void scanOnlyIfChanged();
  }, CFG.worker.watchScanMs);
  watchFallbackTimer.unref?.();
}

function stopSourceWatcher() {
  if (watchDebounceTimer) { clearTimeout(watchDebounceTimer); watchDebounceTimer = null; }
  if (watchFallbackTimer) { clearInterval(watchFallbackTimer); watchFallbackTimer = null; }
  if (sourceWatcher) { try { sourceWatcher.close(); } catch {} sourceWatcher = null; }
}

async function runOnce() {
  return runSerialized(async () => {
    const started = Date.now();
    const snapshot = loadSocialSnapshot(CFG.bridge.socialV2);
    if (snapshot.error) {
      log(`✗ 读不到 social-v2：${snapshot.error}`);
      return;
    }
    if (snapshot.sig) lastScannedSig = snapshot.sig;
    await maybeRecycleSessions(snapshot.conversations);
    if (!CFG.consolidate.enabled) {
      log('记忆整理已由 config.consolidate.enabled=false 关闭；本轮只执行会话退役检查');
      return;
    }
    const targets = targetItems(snapshot.items);
    if (!targets.length) {
      log('没有可整理的会话');
      return;
    }
    for (const item of targets) {
      const itemStarted = Date.now();
      try {
        let r;
        try {
          r = await consolidateConversation(item, snapshot.conversations);
        } catch (error) {
          if (!isDshConnectionError(error)) throw error;
          // DSH 桌面版每次重启会换随机端口。当前批次已经在 pending 中，连接恢复后
          // 原地重试一次不会丢消息；若再次失败则落入外层错误处理，等待下个轮询。
          await reconnectDsh(error);
          r = await consolidateConversation(item, snapshot.conversations);
        }
        recordConsolidationMetric(item, r, Date.now() - itemStarted);
        if (r?.skipped) log(`- ${item.key} 跳过：${r.skipped}`);
        if (r && r.ok === false) log(`✗ ${item.key}：${r.reason}`);
      } catch (error) {
        recordConsolidationMetric(item, { ok: false, reason: 'exception' }, Date.now() - itemStarted);
        // 当前会话失效时只移除该会话绑定，不影响其他群的整理会话
        if (/session|会话|not found|404/i.test(String(error?.message ?? error))) await dropSession(item.key);
        log(`✗ ${item.key} 异常：${error?.message ?? error}`);
      }
    }
    log(`本轮结束，用时 ${Date.now() - started}ms`);
  });
}

// ── 入口 ────────────────────────────────────────────────────────────────────
const once = process.argv.includes('--once');
let stopping = false;

async function main() {
  log(
    `memory-worker 启动（repo=${REPO}，data=${DATA}，poll=${CFG.worker.pollMs}ms，watch=${CFG.worker.watchScanMs}ms，`
    + `batch=${CFG.consolidate.minInformativeMessages}@quiet${Math.round(CFG.consolidate.quietMs / 60000)}m/`
    + `force${CFG.consolidate.forceMessages}/fallback${CFG.consolidate.lowActivityMinMessages}@${Math.round(CFG.consolidate.maxWaitMs / 3_600_000)}h）`
  );
  await connect();
  startStream();

  if (once) {
    await runOnce();
    await shutdown();
    return;
  }

  startSourceWatcher();
  // 首轮延迟 10 秒，之后固定间隔
  await new Promise((r) => setTimeout(r, 10_000));
  while (!stopping) {
    try {
      await runOnce();
    } catch (error) {
      log(`✗ 本轮异常：${error?.message ?? error}`);
      try {
        await connect();
        startStream();
      } catch (e) {
        log(`✗ 重连失败：${e?.message ?? e}`);
      }
    }
    await new Promise((r) => setTimeout(r, CFG.worker.pollMs));
  }
}

async function shutdown() {
  stopping = true;
  stopSourceWatcher();
  try {
    streamAbort?.abort();
  } catch {}
  workerLock.release();
  log('memory-worker 退出');
  process.exit(0);
}

process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());

main().catch((error) => {
  log(`✗ 启动失败：${error?.message ?? error}`);
  workerLock.release();
  process.exit(1);
});
