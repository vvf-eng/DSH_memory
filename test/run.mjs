// 离线单元测试：不依赖 DSH / 桥接 / 网络。
// 用法：node test/run.mjs
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  buildContext
} from '../src/core/context.mjs';
import { apply as applyMemoryPlugin } from '../src/plugin/index.mjs';
import { loadMemoryConfig } from '../src/core/config.mjs';
import { anonymizeConversation, appendMetric, readMetrics, summarizeMetrics } from '../src/core/metrics.mjs';
import { acquireWorkerLock } from '../src/core/worker-lock.mjs';
import {
  beginPendingBatch,
  commitPendingBatch,
  failPendingBatch,
  pendingBatch,
  pendingQueueFile,
  readDeadLetters,
  readPendingQueue,
  scanIntoPending
} from '../src/core/pending.mjs';
import { classifyMessage } from '../src/worker/filter.mjs';
import { evaluatePendingTrigger } from '../src/core/batching.mjs';
import { extractDateRange, inferIntent } from '../src/core/retrieval.mjs';
import { validatePatchEnvelope } from '../src/core/patches.mjs';
import { prepareTransaction, readTransaction, recoverPreparedTransactions } from '../src/core/transaction.mjs';
import { createFactRecord, migrateMembersToSchema3, normalizeFact } from '../src/core/facts.mjs';
import { analyzeToolUsage, applyToolPrunePlan } from '../src/core/tool-usage.mjs';
import {
  getScopedSession,
  nextSessionBudget,
  normalizeConsolidationPool,
  sessionRetirementReason,
  setScopedSession
} from '../src/core/session-budget.mjs';
import { loadSocialSnapshot, readMessages } from '../src/source/social-v2.mjs';
import {
  applyConsolidation,
  buildConsolidationPrompt,
  buildConsolidationRepairPrompt,
  fitMessagesToPromptBudget,
  pruneStateHistory,
  rollbackAudit,
  parseConsolidationOutput,
  dedupeMemberFacts,
  pendingMessages
} from '../src/core/consolidate.mjs';

let pass = 0;
let fail = 0;
const check = (name, cond, extra = '') => {
  if (cond) {
    pass += 1;
    console.log(`✅ ${name}${extra ? ' — ' + extra : ''}`);
  } else {
    fail += 1;
    console.log(`❌ ${name}${extra ? ' — ' + extra : ''}`);
  }
};

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-memory-test-'));
const write = (rel, text) => {
  const file = path.join(root, rel);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, text, 'utf8');
};

// ── 夹具 ────────────────────────────────────────────────────────────────────
write('groups/1234567890/profile.md', `# 群档

<!-- boot:start -->
【本群】测试群，3 人。
【硬规矩】不聊政治、一次最多 2-3 句。
<!-- boot:end -->

## 氛围与常见梗

爱发表情包。
`);
write('groups/1234567890/meta.json', JSON.stringify({ schema: 1, group: 1234567890, name: '测试群', memberCount: 3 }));
write('groups/888/profile.md', `<!-- boot:start -->
【本群】另一个群。
<!-- boot:end -->
`);
write('groups/888/meta.json', JSON.stringify({ schema: 1, group: 888, name: '别的群', memberCount: 2 }));
write('members/1111111111.json', JSON.stringify({
  schema: 2,
  qq: 1111111111,
  displayName: '甲',
  aliases: [],
  global: { tags: [], traits: [], avoid: [], facts: [
    { id: 'g1', text: '喜欢爬山', date: '2026-11-01', source: '', scope: 'global', confidence: 0.9, status: 'active' },
    { id: 'g1-dup', text: '喜欢爬山', date: '2026-10-31', source: '', scope: 'global', confidence: 0.8, status: 'active' },
    { id: 'old', text: '已经过期的旧爱好', date: '2020-01-01', expiresAt: '2020-02-01', source: '', scope: 'global', confidence: 0.9, status: 'active' },
    { id: 'replaced', text: '已被替换的旧事实', date: '2026-10-01', supersededBy: 'g1', source: '', scope: 'global', confidence: 0.9, status: 'active' },
    { id: 'stale', text: '已失效的旧事实', date: '2026-10-01', source: '', scope: 'global', confidence: 0.9, status: 'stale' }
  ] },
  groups: {
    '1234567890': { alias: '甲', relation: '常驻', firstSeen: '2026-11-01', lastSeen: '2026-11-01', interactions: 0, facts: [{ id: 'a1', text: '在群里问过路', date: '2026-11-01', source: 'group:1234567890#seq1', scope: 'group', confidence: 0.8, status: 'active' }] },
    '888': { alias: '甲', relation: '路人', firstSeen: '2026-11-01', lastSeen: '2026-11-01', interactions: 0, facts: [{ id: 'a2', text: '别的群的秘密', date: '2026-11-01', source: 'group:888#seq1', scope: 'group', confidence: 0.8, status: 'active' }] }
  }
}));
write('members/2222222222.json', JSON.stringify({
  schema: 2,
  qq: 2222222222,
  displayName: '小乙',
  aliases: ['乙同学'],
  global: { tags: [], traits: [], avoid: [], facts: [] },
  groups: {
    '1234567890': { alias: '小乙', relation: '常驻', facts: [{ id: 'b1', text: '很怕猫', date: '2026-11-02', scope: 'group', confidence: 0.9, status: 'active' }] }
  }
}));
write('members/3333333333.json', JSON.stringify({
  schema: 2,
  qq: 3333333333,
  displayName: '外群人',
  aliases: [],
  global: { tags: [], facts: [{ id: 'x1', text: '外群人的全局秘密', scope: 'global', confidence: 1, status: 'active' }] },
  groups: { '888': { alias: '外群人', facts: [] } }
}));
write('persona/base.md', '# 底线\n\n- 不暴露路径。\n');
write('persona/dynamic.md', '# 动态\n\n- 最近语气：简短\n');
write('groups/1234567890/timeline/2026-11.md', '## 2026-11-03\n\n- 甲因为迟到和乙吵了一架\n- 甲宣布周末去爬山\n');

const messages = [
  { seq: 10, sender: '甲', userId: '1111111111', text: '我今天去爬山', time: Date.now() - 60000, isSelf: false },
  { seq: 11, sender: '乙', userId: '2222222222', text: '带我一起', time: Date.now() - 30000, isSelf: false },
  { seq: 12, sender: '甲', userId: '1111111111', text: '下次一起啊', time: Date.now() - 10000, isSelf: false }
];

// ── 1. 解析 ────────────────────────────────────────────────────────────────
const parsed = parseConsolidationOutput('```json\n{"timeline":[{"date":"2026-11-07","points":["甲去爬山"]}],"members":[{"qq":"1111111111","alias":"甲","facts":[{"text":"喜欢户外","scope":"global","confidence":0.9,"source_seq":10}]}],"persona":{"tone":"简短","catchphrases":["草"]}}\n```');
check('解析：围栏 JSON 能取出', parsed.ok === true && parsed.timeline.length === 1 && parsed.members.length === 1);
const badJson = parseConsolidationOutput('我觉得挺好的，没有 JSON');
check('解析：无 JSON 时 ok=false', badJson.ok === false);
const contractPrompt = buildConsolidationPrompt({ groupName: '测试群', key: 'group:1234567890', messages });
check('整理契约：明确限定三个 target 字面量', contractPrompt.includes('"timeline"、"member-fact"、"persona"') && contractPrompt.includes('禁止写文件路径'));
check('整理契约：列出 QQ 与可用 sourceSeq 映射', contractPrompt.includes('1111111111=>[10,12]') && contractPrompt.includes('2222222222=>[11]'));
const repairPrompt = buildConsolidationRepairPrompt({ groupName: '测试群', key: 'group:1234567890', messages, errors: ['patches[0].target 非法'] });
check('整理纠错：带回校验错误并重申完整 JSON', repairPrompt.includes('patches[0].target 非法') && repairPrompt.includes('只返回修正后的完整 JSON'));

// ── 2. 应用 + 审计 ─────────────────────────────────────────────────────────
const applied = applyConsolidation({ memDir: root, key: 'group:1234567890', gid: '1234567890', output: parsed, messages });
check('应用：时间线写入', applied.timelinePoints === 1, String(applied.timelinePoints));
check('应用：成员事实写入', applied.memberFacts === 1, String(applied.memberFacts));
check('应用：活跃度累加', applied.interactions === 3, String(applied.interactions));
const auditFile = path.join(root, '.state', 'audit', `${applied.auditId}.json`);
check('审计：记录已落盘', fs.existsSync(auditFile));

// ── 3. 回滚 ────────────────────────────────────────────────────────────────
const rb = rollbackAudit({ memDir: root, id: applied.auditId });
check('回滚：时间线/事实/活跃度都撤了', rb.ok && rb.removed.timeline === 1 && rb.removed.facts === 1 && rb.removed.interactions === 3, JSON.stringify(rb.removed));
const again = rollbackAudit({ memDir: root, id: applied.auditId });
check('回滚：重复回滚被拒绝', again.ok === false);

// ── 3.1 v0.11.0 补丁 schema + 事务 ─────────────────────────────────────────
const patchText = JSON.stringify({
  schema: 1,
  patches: [
    { op: 'add', target: 'timeline', date: new Date(messages[0].time).toISOString().slice(0, 10), value: { text: '甲确认下次和乙一起爬山' } },
    { op: 'add', target: 'member-fact', qq: '1111111111', scope: 'group', groupId: '1234567890', value: { text: '约乙下次一起爬山', confidence: 0.9, sourceSeq: 12 } },
    { op: 'replace', target: 'member-fact', qq: '1111111111', scope: 'global', id: 'g1', value: { text: '长期喜欢户外爬山', confidence: 0.95 } },
    { op: 'remove', target: 'member-fact', qq: '1111111111', scope: 'global', id: 'g1-dup' }
  ]
});
const patchParsed = parseConsolidationOutput(patchText);
check('补丁：schema 1 能解析 add/replace/remove', patchParsed.ok && patchParsed.patchEnvelope?.patches.length === 4);
const patchChecked = validatePatchEnvelope(patchParsed.patchEnvelope, { memDir: root, gid: '1234567890', messages });
check('补丁：本地成员、scope、sourceSeq 校验通过', patchChecked.ok, patchChecked.errors.join('；'));
const badScope = validatePatchEnvelope({ schema: 1, patches: [{ op: 'add', target: 'member-fact', qq: '1111111111', scope: 'group', groupId: '888', value: { text: '越界事实', sourceSeq: 10 } }] }, { memDir: root, gid: '1234567890', messages });
check('补丁：拒绝跨群 scope', badScope.ok === false && badScope.errors.some((error) => error.includes('越界')));
const badTarget = validatePatchEnvelope({ schema: 1, patches: [{ op: 'add', target: 'members/1111111111', value: { text: '路径不是合法目标' } }] }, { memDir: root, gid: '1234567890', messages });
check('补丁：拒绝路径型 target', badTarget.ok === false && badTarget.errors.some((error) => error.includes('target 非法')));
const beforePatchMember = fs.readFileSync(path.join(root, 'members', '1111111111.json'), 'utf8');
const patchApplied = applyConsolidation({ memDir: root, key: 'group:1234567890', gid: '1234567890', output: patchParsed, messages, now: Date.now() + 2000 });
const patchTx = readTransaction(root, patchApplied.auditId);
const patchAudit = JSON.parse(fs.readFileSync(path.join(root, '.state', 'audit', `${patchApplied.auditId}.json`), 'utf8'));
check('事务：写前 prepared 最终进入 committed', patchApplied.transactionStatus === 'committed' && patchTx?.status === 'committed');
check('审计：保存完整 before/after 文件快照', patchAudit.schema === 2 && patchAudit.files.length >= 3 && patchAudit.files.every((entry) => entry.before && entry.after));
const patchRollback = rollbackAudit({ memDir: root, id: patchApplied.auditId });
check('完整回滚：精确恢复成员文件 before', patchRollback.ok && patchRollback.exact === true && fs.readFileSync(path.join(root, 'members', '1111111111.json'), 'utf8') === beforePatchMember);
const schemaFile = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'schemas', 'consolidation-patch.schema.json');
const patchSchema = JSON.parse(fs.readFileSync(schemaFile, 'utf8'));
check('JSON Schema：文件可解析且限制三种 op', patchSchema.properties?.patches?.items?.properties?.op?.enum?.join(',') === 'add,replace,remove');
const recoverFile = path.join(root, 'persona', 'recover-test.md');
fs.writeFileSync(recoverFile, 'before', 'utf8');
prepareTransaction({ memDir: root, id: 'recover-test', targets: [recoverFile] });
fs.writeFileSync(recoverFile, 'partial-write', 'utf8');
const recoveredTx = recoverPreparedTransactions(root);
check('事务：prepared 崩溃恢复按 before 还原', recoveredTx.some((tx) => tx.id === 'recover-test') && fs.readFileSync(recoverFile, 'utf8') === 'before');

// ── 4. 去重 ────────────────────────────────────────────────────────────────
const mf = path.join(root, 'members', '1111111111.json');
const m = JSON.parse(fs.readFileSync(mf, 'utf8'));
m.groups['1234567890'].facts.push({ id: 'dup', text: '在群里问过路的', date: '2026-11-02', source: '', scope: 'group', confidence: 0.5, status: 'active' });
fs.writeFileSync(mf, JSON.stringify(m, null, 2));
const dd = dedupeMemberFacts({ memDir: root, threshold: 0.8, dry: true });
check('去重：能识别近重复（dry 不写）', dd.removed >= 1, String(dd.removed));

// ── 5. 预算 ────────────────────────────────────────────────────────────────
const tight = buildContext({ memDir: root, key: 'group:1234567890', detail: 'full', limit: 12, messages, maxChars: 100 });
check('预算：超限时必须裁剪', tight.truncated === true);
check('预算：群名不被丢', tight.text.includes('测试群'));
check('预算：不超上限', tight.chars <= 100, String(tight.chars));

// ── 6. 跨群隔离 ────────────────────────────────────────────────────────────
const ctxA = buildContext({ memDir: root, key: 'group:1234567890', detail: 'full', limit: 12, messages, maxChars: 2000 });
check('隔离：不出现别的群的事实', !ctxA.text.includes('别的群的秘密'));
check('隔离：本群事实在', ctxA.text.includes('在群里问过路') || ctxA.text.includes('喜欢爬山'));
check('指标元数据：上下文返回事实计数且默认消息为 0', ctxA.stats.memberFacts >= 1 && ctxA.stats.messageCount === 0, JSON.stringify(ctxA.stats));

// ── 7. v0.10 读取检索 ─────────────────────────────────────────────────────
check('读取：默认不重复近期消息', !ctxA.text.includes('下次一起啊') && ctxA.stats.messageCount === 0);
const withRecent = buildContext({ memDir: root, key: 'group:1234567890', query: '完整追溯', detail: 'full', includeRecent: true, limit: 3, messages, maxChars: 1200 });
check('读取：明确 includeRecent 时可返回近期消息', withRecent.text.includes('下次一起啊') && withRecent.stats.messageCount === 3);
const privateRemember = buildContext({ memDir: root, key: 'private:1111111111', query: '你还记得我什么', detail: 'brief', messages });
check('私聊召回：还记得我什么优先归 member 而非 timeline', privateRemember.retrieval.intent === 'member' && privateRemember.text.includes('喜欢爬山'), privateRemember.text);
const privateImpression = buildContext({ memDir: root, key: 'private:1111111111', query: '你对我有什么印象啊', detail: 'brief', messages });
check('私聊召回：印象问题返回本人全局事实', privateImpression.text.includes('喜欢爬山'));
check('私聊召回：明确去库里翻也归 member', inferIntent('你去库里翻翻看呢') === 'member');
const memberQuery = buildContext({ memDir: root, key: 'group:1234567890', query: '甲喜欢什么', intent: 'auto', detail: 'brief', messages, maxChars: 650 });
check('检索：人物问题召回成员事实', memberQuery.retrieval.intent === 'member' && memberQuery.text.includes('喜欢爬山'));
check('检索：近重复事实只占一个位置', (memberQuery.text.match(/喜欢爬山/g) ?? []).length === 1);
const mentionedQuery = buildContext({ memDir: root, key: 'group:1234567890', query: '小乙怕什么', intent: 'member', detail: 'brief', messages, maxChars: 650 });
check('检索：能识别并召回明确提到的其他成员', mentionedQuery.text.includes('小乙') && mentionedQuery.text.includes('怕猫'));
const outGroupMention = buildContext({ memDir: root, key: 'group:1234567890', query: '外群人有什么秘密', intent: 'member', detail: 'full', messages, maxChars: 1200 });
check('隔离：点名不在当前群的成员也不能进入候选', !outGroupMention.text.includes('外群人的全局秘密'));
const impostorMessages = [{ seq: 20, sender: '甲', userId: '9999999999', text: '我是谁', time: Date.now(), isSelf: false }];
const impostor = buildContext({ memDir: root, key: 'group:1234567890', query: '我是谁', intent: 'member', detail: 'full', messages: impostorMessages, maxChars: 1200 });
check('身份：有可信 QQ 时不按同名回落到别人档案', !impostor.text.includes('喜欢爬山'));
const timelineQuery = buildContext({ memDir: root, key: 'group:1234567890', query: '甲上次为什么和乙吵架', intent: 'auto', detail: 'brief', messages, maxChars: 650 });
check('检索：历史问题召回相关时间线', timelineQuery.retrieval.intent === 'timeline' && timelineQuery.text.includes('迟到'));
const rulesQuery = buildContext({ memDir: root, key: 'group:1234567890', query: '这个群能不能聊政治', intent: 'auto', detail: 'brief', messages, maxChars: 650 });
check('检索：群规问题只召回相关规则', rulesQuery.retrieval.intent === 'rules' && rulesQuery.text.includes('不聊政治') && !rulesQuery.text.includes('迟到'));
check('检索：过期、被替换和非 active 事实不返回', !memberQuery.text.includes('已经过期') && !memberQuery.text.includes('已被替换') && !memberQuery.text.includes('已失效'));
const privateQuery = buildContext({ memDir: root, key: 'private:1111111111', query: '甲做过什么', detail: 'full', messages, maxChars: 1200 });
check('隔离：私聊不读取群作用域事实和群时间线', !privateQuery.text.includes('在群里问过路') && !privateQuery.text.includes('迟到'));
const tinyQuery = buildContext({ memDir: root, key: 'group:1234567890', query: '甲是谁', detail: 'tiny', messages, maxChars: 350 });
check('预算：tiny 不超过 350 字', tinyQuery.chars <= 350, String(tinyQuery.chars));
const defaultBudget = buildContext({ memDir: root, key: 'group:1234567890', query: '甲上次怎么了', detail: 'brief', messages, maxChars: 650 });
check('预算：brief 不超过 650 字', defaultBudget.chars <= 650, String(defaultBudget.chars));

// ── 8. 非 QQ key ───────────────────────────────────────────────────────────
const bad = buildContext({ memDir: root, key: 'whatever', detail: 'brief', messages });
check('非法 key 有兜底文本', /会话标识无效/.test(bad.text));

// ── 8. 日期钳制 ────────────────────────────────────────────────────────────
const bogusDate = parseConsolidationOutput(JSON.stringify({
  timeline: [{ date: '2026-05-20', points: ['模型写错的日期'] }],
  members: [],
  persona: null
}));
const clamped = applyConsolidation({ memDir: root, key: 'group:1234567890', gid: '1234567890', output: bogusDate, messages });
check('日期钳制：计入 dateClamped', clamped.dateClamped === 1, String(clamped.dateClamped));
const expectedMonth = (() => {
  const d = new Date(messages[messages.length - 1].time);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
})();
const clampedFile = path.join(root, 'groups', '1234567890', 'timeline', `${expectedMonth}.md`);
check('日期钳制：写进当月而不是 2026-05', fs.existsSync(clampedFile) && fs.readFileSync(clampedFile, 'utf8').includes('模型写错的日期'));
check('日期钳制：没有生成 2026-05 文件', !fs.existsSync(path.join(root, 'groups', '1234567890', 'timeline', '2026-05.md')));

// ── 9. social-v2 序号重置 / 窗口断档 ────────────────────────────────────────
const cursorFile = path.join(root, '.state', 'cursors.json');
const cursors = JSON.parse(fs.readFileSync(cursorFile, 'utf8'));
cursors['group:reset'] = { lastSeq: 784, lastAt: Date.now(), lastCount: 20 };
cursors['group:gap'] = { lastSeq: 100, lastAt: Date.now(), lastCount: 20 };
fs.writeFileSync(cursorFile, JSON.stringify(cursors, null, 2), 'utf8');

const resetWindow = Array.from({ length: 80 }, (_, i) => ({ seq: 121 + i, text: `重置后消息${i}`, time: Date.now() }));
const resetPending = pendingMessages(root, 'group:reset', resetWindow, { max: 120 });
check('序号重置：能识别 sourceLastSeq < cursor', resetPending.sequenceReset === true);
check('序号重置：当前可见窗口不会被旧游标卡死', resetPending.pending.length === 80, String(resetPending.pending.length));
check('序号重置：记录已滚出窗口的缺口', resetPending.gap?.from === 1 && resetPending.gap?.to === 120, JSON.stringify(resetPending.gap));

const gapWindow = [105, 106, 107].map((seq) => ({ seq, text: `断档消息${seq}`, time: Date.now() }));
const gapPending = pendingMessages(root, 'group:gap', gapWindow, { max: 120 });
check('窗口断档：报告 cursor+1 到 firstSeq-1', gapPending.gap?.from === 101 && gapPending.gap?.to === 104, JSON.stringify(gapPending.gap));
check('窗口断档：仍保留当前可恢复消息', gapPending.pending.length === 3);

// ── 10. 配置路径解析 ────────────────────────────────────────────────────────
write('config.json', JSON.stringify({
  dataDir: 'data',
  bridge: { home: path.join(root, 'bridge') },
  worker: { dshClientPath: 'custom/dsh-client.js' }
}));
const loadedCfg = loadMemoryConfig(root);
check('配置：相对 dataDir 以仓库根解析', loadedCfg.dataDir === path.join(root, 'data'), loadedCfg.dataDir);
check(
  '配置：相对 dshClientPath 以 bridge 根解析',
  loadedCfg.worker.dshClientPath === path.join(root, 'bridge', 'custom', 'dsh-client.js'),
  loadedCfg.worker.dshClientPath
);
check('配置：自适应批处理默认值正确',
  loadedCfg.consolidate.minInformativeMessages === 30
  && loadedCfg.consolidate.forceMessages === 100
  && loadedCfg.consolidate.quietMs === 900000
  && loadedCfg.consolidate.maxWaitMs === 43200000);
check('配置：整理会话预算与工具观察门槛正确', loadedCfg.worker.consolidationSession.maxBatches === 4 && loadedCfg.worker.consolidationSession.maxSteps === 12 && loadedCfg.worker.consolidationSession.maxTokens === 100000 && loadedCfg.toolUsage.observationDays === 14);

// ── 11. 插件身份复核失败关闭 ────────────────────────────────────────────────
write('config.json', JSON.stringify({ dataDir: root, bridge: { home: path.join(root, 'missing-bridge') } }));
let registeredTool = null;
applyMemoryPlugin({ tools: { register: (tool) => { registeredTool = tool; } } }, { memDir: root });
check('工具提示：同一消息防重复，但 wait 新消息可再次调用', /普通问候/.test(registeredTool.description) && /同一条用户消息/.test(registeredTool.description) && /wait_for_messages/.test(registeredTool.description) && /可以再次调用/.test(registeredTool.description) && !/同一轮最多一次/.test(registeredTool.description));
check('工具提示：印象/记得我/查库必须调用且禁止假装查过', /有什么印象/.test(registeredTool.description) && /还记得我什么/.test(registeredTool.description) && /必须先调用/.test(registeredTool.description) && /不得声称查过却不调用/.test(registeredTool.description));
check('工具提示：询问别人记忆也必须查库，禁止先说不记得', /你记得X吗/.test(registeredTool.description) && /X是谁/.test(registeredTool.description) && /X干过什么/.test(registeredTool.description) && /提问者是谁不影响调用/.test(registeredTool.description) && /禁止直接回答“不记得\/不知道\/没印象”/.test(registeredTool.description));
const denied = await registeredTool.execute({}, { agent: { id: 'qqb-g-1234567890-0123456789abcdef0123456789abcdef' } });
check('身份隔离：sessions.json 不可读时失败关闭', /不在记忆库范围内/.test(String(denied?.text ?? '')));

const validSessionId = 'qqb-g-1234567890-0123456789abcdef0123456789abcdef';
write('bridge/state/sessions.json', JSON.stringify({ sessions: { 'group:1234567890': validSessionId } }));
write('bridge/state/social-v2.json', JSON.stringify({ conversations: { 'group:1234567890': { recentMessages: messages } } }));
write('config.json', JSON.stringify({
  dataDir: root,
  bridge: { home: path.join(root, 'bridge') },
  context: { maxChars: 650, cacheTtlMs: 120000, cacheEntries: 20 }
}));
const pluginFirst = await registeredTool.execute({ query: '甲喜欢什么', intent: 'auto' }, { agent: { id: validSessionId }, turn: { id: 'turn-1' } });
const pluginSecond = await registeredTool.execute({ query: '甲喜欢什么', intent: 'auto' }, { agent: { id: validSessionId }, turn: { id: 'turn-1' } });
check('插件检索：合法会话能按 query 召回', String(pluginFirst?.text ?? '').includes('喜欢爬山'));
check('插件检索：默认不把近期消息重复返回', !String(pluginFirst?.text ?? '').includes('下次一起啊'));
const pluginMetricRows = fs.readFileSync(path.join(root, '.state', 'metrics.jsonl'), 'utf8').trim().split(/\r?\n/).map((line) => JSON.parse(line));
check('插件缓存：同一 turn/seq/query 第二次复用', pluginSecond?.text === pluginFirst?.text && pluginMetricRows.some((row) => row.type === 'memory-read' && row.cached === true));

// ── 12. 匿名指标 ──────────────────────────────────────────────────────────
const metricKey = anonymizeConversation(root, 'group:1234567890');
check('指标：会话只保留 HMAC 代号', /^hmac:[0-9a-f]{20}$/.test(metricKey) && !metricKey.includes('1234567890'), metricKey);
appendMetric(root, {
  type: 'memory-read', conversation: metricKey, status: 'success', reason: 'ok', detail: 'brief',
  durationMs: 12, returnedChars: 321, memberFacts: 2, timelinePoints: 1, messageCount: 3
}, { maxBytes: 64 * 1024, keepFiles: 2 });
appendMetric(root, {
  type: 'consolidate', conversation: metricKey, status: 'success', reason: 'ok', durationMs: 34,
  rawMessages: 5, memberFacts: 1, timelinePoints: 2, usage: { inputTokens: 999, cacheMiss: 100, output: 20 }
});
appendMetric(root, {
  type: 'consolidate', conversation: metricKey, status: 'skipped', reason: 'too-few', durationMs: 1,
  rawMessages: 5
});
const metricRaw = fs.readFileSync(path.join(root, '.state', 'metrics.jsonl'), 'utf8');
check('指标：落盘不含真实群号和消息正文', !metricRaw.includes('1234567890') && !metricRaw.includes('我今天去爬山'));
const metricSummary = summarizeMetrics(readMetrics(root, { days: 1 }));
check('指标：可汇总且不把重复轮询候选算成模型输入', metricSummary.reads.success >= 1 && metricSummary.consolidate.memberFacts === 1 && metricSummary.consolidate.rawMessages === 5, JSON.stringify(metricSummary));
for (let i = 0; i < 260; i += 1) {
  appendMetric(root, { type: 'memory-read', conversation: metricKey, status: 'success', reason: 'ok', detail: 'brief', returnedChars: i }, { maxBytes: 64 * 1024, keepFiles: 2 });
}
check('指标：超过上限会轮转', fs.existsSync(path.join(root, '.state', 'metrics.jsonl.1')));

// ── 13. worker 单实例锁 ───────────────────────────────────────────────────
const lockA = acquireWorkerLock(root, { heartbeatMs: 5000, staleMs: 30000, version: 'test' });
const lockB = acquireWorkerLock(root, { heartbeatMs: 5000, staleMs: 30000, version: 'test' });
check('worker 锁：首个实例能获取', lockA.ok === true);
check('worker 锁：第二个实例被拒绝', lockB.ok === false && lockB.reason === 'already-running');
lockA.release();
const lockC = acquireWorkerLock(root, { heartbeatMs: 5000, staleMs: 30000, version: 'test' });
check('worker 锁：释放后可以重新获取', lockC.ok === true);
lockC.release();
const freshDeadLockFile = path.join(root, '.state', 'worker.lock');
fs.writeFileSync(freshDeadLockFile, JSON.stringify({
  schema: 1,
  pid: 2147483647,
  hostname: os.hostname(),
  version: 'dead-test',
  nonce: 'dead',
  startedAt: new Date().toISOString(),
  heartbeatAt: new Date().toISOString()
}, null, 2));
const lockAfterCrash = acquireWorkerLock(root, { heartbeatMs: 5000, staleMs: 30000, version: 'test' });
check('worker 锁：同机 PID 已死时立即回收新鲜锁', lockAfterCrash.ok === true);
lockAfterCrash.release();

// ── 14. v0.10.1 保守过滤 + 持久 pending ────────────────────────────────────
check('过滤：空消息/撤回/命令确定丢弃',
  classifyMessage({ text: '' }).level === 'ignore'
  && classifyMessage({ text: '有内容', recalled: true }).level === 'ignore'
  && classifyMessage({ text: '/reset' }).level === 'ignore');
check('过滤：纯表情与短确认不入长期队列',
  classifyMessage({ text: '😀😀' }).level === 'ignore'
  && classifyMessage({ text: '哈哈哈' }).level === 'low');
check('过滤：长期信号标 important，拿不准的短文本仍保留',
  classifyMessage({ text: '我换工作了' }).level === 'important'
  && classifyMessage({ text: '猫猫' }).level === 'normal');

const queueKey = 'group:424242';
const queueMessages = [
  { seq: 1, userId: '1', sender: '甲', text: '', time: 1000 },
  { seq: 2, userId: '1', sender: '甲', text: '哈哈哈', time: 2000 },
  { seq: 3, userId: '1', sender: '甲', text: '我换工作了', time: 3000 },
  { seq: 4, userId: '2', sender: '乙', text: '猫猫', time: 4000 },
  { seq: 5, userId: '2', sender: '乙', text: '/reset', time: 5000 },
  { seq: 6, userId: '2', sender: '乙', text: '[图片]', time: 6000 },
  { seq: 7, userId: '2', sender: '乙', text: '下周准备去旅行', time: 7000 }
];
const scannedQueue = scanIntoPending({ memDir: root, key: queueKey, messages: queueMessages, now: 10_000 });
check('队列：无价值消息也推进 scan', scannedQueue.queue.scan.seq === 7 && scannedQueue.counts.scanned === 7, JSON.stringify(scannedQueue.counts));
check('队列：只有 normal/important 持久入队', scannedQueue.queue.pending.length === 3 && scannedQueue.counts.enqueued === 3);
check('队列：不只存游标，还保存正文、分类、统计与历史',
  fs.existsSync(pendingQueueFile(root, queueKey))
  && scannedQueue.queue.pending[0].text === '我换工作了'
  && scannedQueue.queue.pending[0].level === 'important'
  && scannedQueue.queue.history.length >= 2);
const rescannedQueue = scanIntoPending({ memDir: root, key: queueKey, messages: queueMessages, now: 11_000 });
check('队列：重复扫描不会重复入队', rescannedQueue.queue.pending.length === 3 && rescannedQueue.counts.scanned === 0);

const batch1 = beginPendingBatch({ memDir: root, key: queueKey, maxMessages: 2, now: 12_000 });
check('队列：prompt 前持久记录批次和 attempt', batch1.items.length === 2 && batch1.items.every((item) => item.attempts === 1) && Boolean(batch1.queue.inflight));
const failedQueue = failPendingBatch({ memDir: root, key: queueKey, batchId: batch1.batchId, reason: 'parse-failed', now: 13_000 });
check('队列：整理失败不出队并记录失败', failedQueue.pending.length === 3 && failedQueue.failures.count === 1 && failedQueue.pending[0].lastError === 'parse-failed');
const batch2 = beginPendingBatch({ memDir: root, key: queueKey, maxMessages: 2, now: 14_000 });
check('队列：失败批次下轮重试同一消息', batch2.batchId === batch1.batchId && batch2.items.every((item) => item.attempts === 2));
const committedQueue = commitPendingBatch({ memDir: root, key: queueKey, batchId: batch2.batchId, result: { timelinePoints: 1 }, now: 15_000 });
check('队列：成功后只移除已提交批次', committedQueue.committed.length === 2 && committedQueue.queue.pending.length === 1 && committedQueue.queue.batches.length === 1);
const sourceGone = scanIntoPending({ memDir: root, key: queueKey, messages: [], now: 16_000 });
check('队列：social-v2 窗口消失后 pending 仍保留', sourceGone.queue.pending.length === 1);

const resetScan = scanIntoPending({
  memDir: root,
  key: queueKey,
  messages: [
    { seq: 1, userId: '3', sender: '丙', text: '新一代的有效消息', time: 17_000 },
    { seq: 2, userId: '3', sender: '丙', text: '嗯', time: 18_000 }
  ],
  now: 19_000
});
check('队列：seq 回退开启新 generation 且旧 pending 不丢', resetScan.sequenceReset === true && resetScan.queue.generation === 1 && resetScan.queue.pending.length === 2);
const gapScan = scanIntoPending({ memDir: root, key: queueKey, messages: [{ seq: 5, userId: '3', sender: '丙', text: '断档后仍保存', time: 20_000 }], now: 21_000 });
check('队列：窗口断档写入恢复状态并保存可见消息', gapScan.gap?.from === 3 && gapScan.gap?.to === 4 && gapScan.queue.recovery.pending === true && gapScan.queue.pending.some((item) => item.seq === 5));
check('队列：批次不混合不同 seq generation', new Set(pendingBatch(gapScan.queue, 120).map((item) => item.generation)).size === 1);
check('队列：文件可独立重读恢复', readPendingQueue(root, queueKey).pending.length === gapScan.queue.pending.length);

// ── 15. v0.10.2 自适应批处理（只看 pending）───────────────────────────────
const batchNow = 100 * 3_600_000;
const adaptiveCfg = { minInformativeMessages: 30, forceMessages: 100, lowActivityMinMessages: 10, quietMs: 900_000, maxWaitMs: 43_200_000 };
const fakePending = (count, { lastActivityAgo = 0, lastCommittedAgo = 0, recovery = false } = {}) => ({
  pending: Array.from({ length: count }, (_, i) => ({ id: `p${i}`, enqueuedAt: batchNow - Math.max(lastCommittedAgo, 60_000), time: batchNow - lastActivityAgo })),
  scan: { lastMessageAt: batchNow - lastActivityAgo },
  memory: { lastCommittedAt: lastCommittedAgo ? batchNow - lastCommittedAgo : 0 },
  recovery: { pending: recovery }
});
check('批处理：没有 pending 不触发', evaluatePendingTrigger(fakePending(0), adaptiveCfg, batchNow).reason === 'no-informative');
const active30 = evaluatePendingTrigger(fakePending(30, { lastActivityAgo: 5 * 60_000, lastCommittedAgo: 2 * 3_600_000 }), adaptiveCfg, batchNow);
check('批处理：30 条但群仍活跃时等待安静', active30.ready === false && active30.reason === 'waiting-quiet');
const quiet30 = evaluatePendingTrigger(fakePending(30, { lastActivityAgo: 15 * 60_000, lastCommittedAgo: 2 * 3_600_000 }), adaptiveCfg, batchNow);
check('批处理：30 条且安静 15 分钟触发', quiet30.ready === true && quiet30.trigger === 'quiet');
const active100 = evaluatePendingTrigger(fakePending(100, { lastActivityAgo: 10_000, lastCommittedAgo: 2 * 3_600_000 }), adaptiveCfg, batchNow);
check('批处理：100 条无需等待安静强制触发', active100.ready === true && active100.trigger === 'force');
const lowActivity10 = evaluatePendingTrigger(fakePending(10, { lastActivityAgo: 1_000, lastCommittedAgo: 12 * 3_600_000 }), adaptiveCfg, batchNow);
check('批处理：低活跃 10 条满 12 小时兜底', lowActivity10.ready === true && lowActivity10.trigger === 'max-wait');
const old9 = evaluatePendingTrigger(fakePending(9, { lastActivityAgo: 20 * 60_000, lastCommittedAgo: 24 * 3_600_000 }), adaptiveCfg, batchNow);
check('批处理：不足 10 条即使很久也继续积累', old9.ready === false && old9.reason === 'waiting-min-informative');
const recovery1 = evaluatePendingTrigger(fakePending(1, { recovery: true }), adaptiveCfg, batchNow);
check('批处理：reset/gap 恢复 pending 立即触发', recovery1.ready === true && recovery1.trigger === 'recovery');

// ── 16. v0.11.1 schema 2 → 3 生命周期迁移 ─────────────────────────────────
const migrateRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-memory-migrate-'));
fs.mkdirSync(path.join(migrateRoot, 'members'), { recursive: true });
const schema2Member = {
  schema: 2, qq: 4444444444, displayName: '迁移样本', aliases: [],
  global: { facts: [{ id: 'old-life', text: '旧生命周期事实', date: '2026-01-01', source: 'group:123#seq9', scope: 'global', confidence: 0.8, status: 'active', expiresAt: '2027-01-01', supersededBy: 'new-life' }] },
  groups: { '123': { facts: [{ id: 'group-old', text: '旧群事实', date: '2026-02-01', source: 'group:123#seq10', scope: 'group', status: 'active' }] } }
};
fs.writeFileSync(path.join(migrateRoot, 'members', '4444444444.json'), JSON.stringify(schema2Member), 'utf8');
const migrationNow = Date.UTC(2026, 10, 9, 0, 0, 0);
const dryMigration = migrateMembersToSchema3({ memDir: migrateRoot, dry: true, observationDays: 14, now: migrationNow });
check('迁移：dry-run 不改 schema、不建备份', dryMigration.migrated === 1 && JSON.parse(fs.readFileSync(path.join(migrateRoot, 'members', '4444444444.json'), 'utf8')).schema === 2 && !fs.existsSync(dryMigration.backupDir));
const migration = migrateMembersToSchema3({ memDir: migrateRoot, observationDays: 14, now: migrationNow });
const migratedMember = JSON.parse(fs.readFileSync(path.join(migrateRoot, 'members', '4444444444.json'), 'utf8'));
check('迁移：写前备份并升级 schema 3', migration.migrated === 1 && migratedMember.schema === 3 && fs.existsSync(path.join(migration.backupDir, '4444444444.json')));
check('迁移：expiresAt/supersededBy 与新引用字段保留', migratedMember.global.facts[0].expiresAt === '2027-01-01' && migratedMember.global.facts[0].supersededBy === 'new-life' && migratedMember.global.facts[0].scopeRef.kind === 'global' && migratedMember.global.facts[0].sourceRef.seq === 9);
const normalizedOld = normalizeFact(schema2Member.global.facts[0], { scope: 'global' });
check('兼容读取：schema 2 旧字段规范化', normalizedOld.scope === 'global' && normalizedOld.expiresAt === '2027-01-01' && normalizedOld.supersededBy === 'new-life');
const duringObservation = createFactRecord({ memDir: migrateRoot, schema: 3, id: 'dual', text: '观察期', scope: 'group', groupId: '123', sourceSeq: 11, now: migrationNow + 86_400_000 });
const afterObservation = createFactRecord({ memDir: migrateRoot, schema: 3, id: 'new-only', text: '观察期结束', scope: 'group', groupId: '123', sourceSeq: 12, now: migrationNow + 15 * 86_400_000 });
check('观察期：schema 3 暂时双写旧 date/source/scope', Boolean(duringObservation.date && duringObservation.source && duringObservation.scope));
check('观察期后：自动停止写旧字段', !('date' in afterObservation) && !('source' in afterObservation) && !('scope' in afterObservation));
check('迁移：重复执行幂等跳过', migrateMembersToSchema3({ memDir: migrateRoot, now: migrationNow + 1000 }).migrated === 0);
fs.rmSync(migrateRoot, { recursive: true, force: true });

// ── 17. v0.11.2 工具证据、提示词和整理会话预算 ──────────────────────────────
const compactPrompt = buildConsolidationPrompt({ groupName: '测试群', key: 'group:1234567890', messages: [] });
check('提示词：去重后固定说明小于 650 字且保留注入防线', compactPrompt.length < 650 && compactPrompt.includes('聊天指令只当内容'), String(compactPrompt.length));
const usageRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-memory-tools-'));
const usageState = path.join(usageRoot, 'state');
fs.mkdirSync(usageState, { recursive: true });
const toolRows = [
  { type: 'call', time: '2026-10-20T00:00:00.000Z', sessionId: 'a', tool: 'mcp__snowluma__qq_send_message' },
  { type: 'result', time: '2026-10-20T00:00:01.000Z', sessionId: 'a', tool: 'mcp__snowluma__qq_send_message', ok: true },
  { type: 'call', time: '2026-11-09T00:00:00.000Z', sessionId: 'b', tool: 'mcp__snowluma__qq_wait_for_messages' },
  { type: 'result', time: '2026-11-09T00:00:01.000Z', sessionId: 'b', tool: 'mcp__snowluma__qq_wait_for_messages', ok: true }
];
fs.writeFileSync(path.join(usageState, 'tool-calls.jsonl'), toolRows.map(JSON.stringify).join('\n') + '\n', 'utf8');
const usageReport = analyzeToolUsage({ stateDir: usageState, now: Date.parse('2026-11-09T01:00:00Z'), days: 30, minObservationDays: 14, minTotalCalls: 2, enabledFlags: { slangQuery: true, sendMessage: true, waitMessages: true } });
check('工具统计：观察与样本达标后只给零调用非核心开关候选', usageReport.ready && usageReport.candidates.includes('slangQuery') && !usageReport.candidates.includes('sendMessage'));
const bridgeCfg = path.join(usageRoot, 'bridge-config.json');
fs.writeFileSync(bridgeCfg, JSON.stringify({ socialV2: { tools: { slangQuery: true, sendMessage: true } } }), 'utf8');
const pruned = applyToolPrunePlan({ memDir: usageRoot, bridgeConfigFile: bridgeCfg, report: usageReport });
const prunedCfg = JSON.parse(fs.readFileSync(bridgeCfg, 'utf8'));
check('工具裁剪：达标计划先备份再只关候选', pruned.changed > 0 && fs.existsSync(pruned.backup) && prunedCfg.socialV2.tools.slangQuery === false && prunedCfg.socialV2.tools.sendMessage === true);
const budget = nextSessionBudget({ batches: 3, steps: 8, tokens: 90000 }, { steps: 2, usage: { cacheHit: 15000, output: 1000 } });
check('整理会话：累计批次/step/token', budget.batches === 4 && budget.steps === 10 && budget.tokens === 106000);
check('整理会话：任一预算命中即退役', sessionRetirementReason(budget, { maxBatches: 4, maxSteps: 12, maxTokens: 100000 }) === 'batches');
fs.rmSync(usageRoot, { recursive: true, force: true });

// ── 18. 群名片回填/本群旧名隔离 + 补丁失败指标统计 ──────────────────────────
const newMemberMsgs1 = [
  { seq: 50, sender: '初号机', userId: '5555555555', text: '我平时喜欢拼模型', time: Date.now() - 20_000, isSelf: false }
];
const newMemberPatch1 = parseConsolidationOutput(JSON.stringify({
  schema: 1,
  patches: [
    { op: 'add', target: 'member-fact', qq: '5555555555', scope: 'global', value: { text: '喜欢拼模型', confidence: 0.9, sourceSeq: 50 } }
  ]
}));
applyConsolidation({ memDir: root, key: 'group:1234567890', gid: '1234567890', output: newMemberPatch1, messages: newMemberMsgs1 });
const createdMember = JSON.parse(fs.readFileSync(path.join(root, 'members', '5555555555.json'), 'utf8'));
check(
  '群名片：新建成员写入本群 alias 且不污染全局 displayName/aliases',
  createdMember.groups?.['1234567890']?.alias === '初号机'
  && createdMember.displayName === ''
  && Array.isArray(createdMember.aliases) && createdMember.aliases.length === 0
);
const askByNewCard = buildContext({
  memDir: root,
  key: 'group:1234567890',
  query: '初号机喜欢什么',
  intent: 'member',
  detail: 'brief',
  messages: [{ seq: 51, sender: '小乙', userId: '2222222222', text: '初号机喜欢什么', time: Date.now() - 15_000, isSelf: false }]
});
check('群名片：新建成员可按群名片点名召回并显示名称', askByNewCard.text.includes('· 初号机：喜欢拼模型'), askByNewCard.text);

const renameMsgs = [
  { seq: 52, sender: '二号机', userId: '5555555555', text: '我改个群名片冒泡', time: Date.now() - 10_000, isSelf: false }
];
const emptyPatch = parseConsolidationOutput(JSON.stringify({ schema: 1, patches: [] }));
applyConsolidation({ memDir: root, key: 'group:1234567890', gid: '1234567890', output: emptyPatch, messages: renameMsgs, now: Date.now() + 5_000 });
const renamedMember = JSON.parse(fs.readFileSync(path.join(root, 'members', '5555555555.json'), 'utf8'));
check(
  '群名片：改名后旧名仅保留在本群 aliases',
  renamedMember.groups?.['1234567890']?.alias === '二号机'
  && Array.isArray(renamedMember.groups?.['1234567890']?.aliases)
  && renamedMember.groups['1234567890'].aliases.includes('初号机')
  && renamedMember.displayName === ''
  && renamedMember.aliases.length === 0
);
const askByOldCard = buildContext({
  memDir: root,
  key: 'group:1234567890',
  query: '初号机喜欢什么',
  intent: 'member',
  detail: 'brief',
  messages: [{ seq: 53, sender: '小乙', userId: '2222222222', text: '初号机喜欢什么', time: Date.now() - 8_000, isSelf: false }]
});
check('本群旧称：用旧群名片提问仍能召回该成员', askByOldCard.text.includes('二号机') && askByOldCard.text.includes('喜欢拼模型'), askByOldCard.text);
const askOldAlias = buildContext({
  memDir: root,
  key: 'group:1234567890',
  query: '二号机以前叫什么',
  intent: 'auto',
  detail: 'brief',
  messages: [{ seq: 54, sender: '小乙', userId: '2222222222', text: '二号机以前叫什么', time: Date.now() - 6_000, isSelf: false }]
});
check('本群旧称：询问以前叫什么归 member 并返回旧称', askOldAlias.retrieval.intent === 'member' && askOldAlias.text.includes('旧称：初号机'), askOldAlias.text);

applyConsolidation({
  memDir: root,
  key: 'group:888',
  gid: '888',
  output: parseConsolidationOutput(JSON.stringify({
    schema: 1,
    patches: [{ op: 'add', target: 'timeline', date: new Date().toISOString().slice(0, 10), value: { text: '路人五在群里签到' } }]
  })),
  messages: [{ seq: 1, sender: '路人五', userId: '5555555555', text: '大家好', time: Date.now() - 5_000, isSelf: false }],
  now: Date.now() + 10_000
});
const crossGroupOldName = buildContext({
  memDir: root,
  key: 'group:888',
  query: '初号机喜欢什么',
  intent: 'member',
  detail: 'full',
  messages: [{ seq: 2, sender: '外群人', userId: '3333333333', text: '初号机喜欢什么', time: Date.now() - 3_000, isSelf: false }]
});
check('隔离：他群不可通过原群旧名或名片反查成员', !crossGroupOldName.text.includes('喜欢拼模型') && !crossGroupOldName.text.includes('初号机') && !crossGroupOldName.text.includes('二号机'), crossGroupOldName.text);
const privateNoGroupAlias = buildContext({
  memDir: root,
  key: 'private:5555555555',
  query: '你还记得我什么',
  intent: 'auto',
  detail: 'full',
  messages: [{ seq: 1, sender: '五号私聊', userId: '5555555555', text: '你还记得我什么', time: Date.now() - 2_000, isSelf: false }]
});
check('隔离：私聊不暴露任何群名片或群旧称', privateNoGroupAlias.text.includes('喜欢拼模型') && !privateNoGroupAlias.text.includes('初号机') && !privateNoGroupAlias.text.includes('二号机'), privateNoGroupAlias.text);

const patchMetricRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-memory-patch-metrics-'));
const patchConv = anonymizeConversation(patchMetricRoot, 'group:1234567890');
appendMetric(patchMetricRoot, { type: 'consolidate', conversation: patchConv, status: 'failed', reason: 'patch-invalid', rawMessages: 30, informativeMessages: 28 });
appendMetric(patchMetricRoot, { type: 'consolidate', conversation: patchConv, status: 'failed', reason: 'repair-prompt-rejected', rawMessages: 15, informativeMessages: 14 });
appendMetric(patchMetricRoot, { type: 'consolidate', conversation: patchConv, status: 'failed', reason: 'prompt-rejected', rawMessages: 20, informativeMessages: 20 });
appendMetric(patchMetricRoot, { type: 'consolidate', conversation: patchConv, status: 'skipped', reason: 'waiting-quiet', rawMessages: 0, informativeMessages: 30 });
const patchMetricRows = readMetrics(patchMetricRoot, { days: 1 });
check(
  '指标：保留 patch-invalid 与 repair-prompt-rejected 原因',
  patchMetricRows.some((r) => r.reason === 'patch-invalid') && patchMetricRows.some((r) => r.reason === 'repair-prompt-rejected')
);
const patchMetricSummary = summarizeMetrics(patchMetricRows);
check(
  '指标：两种补丁失败计入模型输入条数且排除未执行回合的失败/跳过',
  patchMetricSummary.consolidate.patchFailed === 2
  && patchMetricSummary.consolidate.rawMessages === 45
  && patchMetricSummary.consolidate.informativeMessages === 42,
  JSON.stringify(patchMetricSummary.consolidate)
);
fs.rmSync(patchMetricRoot, { recursive: true, force: true });

// ── 19. 全量增强回归（防吞消息 / 跨群回滚 / 退避死信 / 短回复引用 / 旧事实闭环 / 分词指代泛查 / 私聊与人设隔离 / 会话池）──
const budgetKey = 'group:909090';
const longMessages = Array.from({ length: 20 }, (_, i) => ({
  seq: i + 1,
  sender: '刷屏王',
  userId: '7777777777',
  text: `第${i + 1}条较长的有效聊天内容，用于验证字符预算前置裁剪不会静默丢失尾部消息`,
  time: 100_000 + i * 1000,
  isSelf: false
}));
scanIntoPending({ memDir: root, key: budgetKey, messages: longMessages, now: 200_000 });
const budgetedBatch = beginPendingBatch({ memDir: root, key: budgetKey, maxMessages: 100, maxChars: 550, now: 201_000 });
const expectedFit = fitMessagesToPromptBudget(longMessages, { maxChars: 550 });
check(
  '防吞消息：beginPendingBatch 按 maxChars 裁剪且与 prompt 对齐',
  budgetedBatch.items.length > 0 && budgetedBatch.items.length < 20 && budgetedBatch.items.length === expectedFit.length,
  `${budgetedBatch.items.length}/20`
);
const afterBudgetCommit = commitPendingBatch({ memDir: root, key: budgetKey, batchId: budgetedBatch.batchId, now: 202_000 });
check('防吞消息：超出 prompt 预算的尾部消息留在 pending 等下一批', afterBudgetCommit.queue.pending.length === 20 - expectedFit.length);

// 跨群独立游标精确回滚 + 同群后续修改后的逻辑回滚兜底 + GC
const rbGroupAMsgs = [{ seq: 80, sender: '甲', userId: '1111111111', text: '准备学摄影', time: Date.now() - 40_000, isSelf: false }];
const rbGroupAPatch = parseConsolidationOutput(JSON.stringify({
  schema: 1,
  patches: [{ op: 'add', target: 'member-fact', qq: '1111111111', scope: 'group', groupId: '1234567890', value: { text: '准备学摄影', confidence: 0.85, sourceSeq: 80 } }]
}));
const auditCrossA = applyConsolidation({ memDir: root, key: 'group:1234567890', gid: '1234567890', output: rbGroupAPatch, messages: rbGroupAMsgs, now: Date.now() + 20_000 });
applyConsolidation({
  memDir: root,
  key: 'group:888',
  gid: '888',
  output: parseConsolidationOutput(JSON.stringify({ schema: 1, patches: [{ op: 'add', target: 'timeline', date: new Date().toISOString().slice(0, 10), value: { text: '888群独立事件' } }] })),
  messages: [{ seq: 10, sender: '外群人', userId: '3333333333', text: '888群独立发言', time: Date.now() - 30_000, isSelf: false }],
  now: Date.now() + 25_000
});
const rbCrossRes = rollbackAudit({ memDir: root, id: auditCrossA.auditId });
check('独立游标：其他群整理后本群仍可执行精确事务回滚', rbCrossRes.ok === true && rbCrossRes.exact === true);

const batch1Same = applyConsolidation({
  memDir: root,
  key: 'group:1234567890',
  gid: '1234567890',
  output: parseConsolidationOutput(JSON.stringify({
    schema: 1,
    patches: [{ op: 'add', target: 'member-fact', qq: '1111111111', scope: 'group', groupId: '1234567890', value: { text: '误记的旧批次事实', confidence: 0.7, sourceSeq: 80 } }]
  })),
  messages: rbGroupAMsgs,
  now: Date.now() + 30_000
});
applyConsolidation({
  memDir: root,
  key: 'group:1234567890',
  gid: '1234567890',
  output: parseConsolidationOutput(JSON.stringify({
    schema: 1,
    patches: [{ op: 'add', target: 'member-fact', qq: '1111111111', scope: 'group', groupId: '1234567890', value: { text: '后续批次的新事实应保留', confidence: 0.9, sourceSeq: 81 } }]
  })),
  messages: [{ seq: 81, sender: '甲', userId: '1111111111', text: '后续批次的新事实应保留', time: Date.now() - 20_000, isSelf: false }],
  now: Date.now() + 35_000
});
const rbLogical = rollbackAudit({ memDir: root, id: batch1Same.auditId });
const mAfterLogical = JSON.parse(fs.readFileSync(path.join(root, 'members', '1111111111.json'), 'utf8'));
const gFactsTexts = mAfterLogical.groups['1234567890'].facts.map((f) => f.text);
check(
  '逻辑回滚：同群后续批次修改文件后仍可精确剔除目标批次事实且保留新批次',
  rbLogical.ok === true && rbLogical.fallback === 'logical' && !gFactsTexts.includes('误记的旧批次事实') && gFactsTexts.includes('后续批次的新事实应保留')
);

const gcRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-memory-gc-'));
const oldTs = Date.now() - 40 * 86_400_000;
fs.mkdirSync(path.join(gcRoot, '.backups'), { recursive: true });
const oldBak = path.join(gcRoot, '.backups', 'old.bak');
fs.writeFileSync(oldBak, 'old-backup', 'utf8');
fs.utimesSync(oldBak, new Date(oldTs), new Date(oldTs));
fs.mkdirSync(path.join(gcRoot, '.state', 'transactions'), { recursive: true });
fs.writeFileSync(path.join(gcRoot, '.state', 'transactions', '20260101-000000-1.json'), JSON.stringify({ id: 'old-tx', status: 'committed', committedAt: oldTs, files: [] }), 'utf8');
fs.writeFileSync(path.join(gcRoot, '.state', 'transactions', '20260101-000001-1.json'), JSON.stringify({ id: 'prep-tx', status: 'prepared', preparedAt: oldTs, files: [] }), 'utf8');
fs.writeFileSync(path.join(gcRoot, '.state', 'transactions', '20261125-000000-1.json'), JSON.stringify({ id: 'recent-tx', status: 'committed', committedAt: Date.now(), files: [] }), 'utf8');
fs.mkdirSync(path.join(gcRoot, '.state', 'audit'), { recursive: true });
fs.writeFileSync(path.join(gcRoot, '.state', 'audit', '20260101-000000-1.json'), JSON.stringify({ id: 'old-audit', at: oldTs, files: [{ before: { text: 'big' }, after: { text: 'big2' } }], facts: [{ id: 'f1' }] }), 'utf8');
fs.writeFileSync(path.join(gcRoot, '.state', 'audit', '20261125-000000-1.json'), JSON.stringify({ id: 'new-audit', at: Date.now(), files: [{ before: { text: 'keep' }, after: { text: 'keep' } }] }), 'utf8');
const gcRes = pruneStateHistory({ memDir: gcRoot, days: 30, keepLatest: 1, dry: false });
const compactedAudit = JSON.parse(fs.readFileSync(path.join(gcRoot, '.state', 'audit', '20260101-000000-1.json'), 'utf8'));
check(
  'GC 清理：删除超期备份与已完成事务、压缩旧审计快照且保护 prepared 事务',
  gcRes.backupsRemoved === 1
  && gcRes.transactionsRemoved === 1
  && gcRes.auditsCompacted === 1
  && fs.existsSync(path.join(gcRoot, '.state', 'transactions', '20260101-000001-1.json'))
  && !('files' in compactedAudit)
  && compactedAudit.facts.length === 1
);
fs.rmSync(gcRoot, { recursive: true, force: true });

// 失败指数退避与毒丸死信隔离
const poisonKey = 'group:717171';
scanIntoPending({
  memDir: root,
  key: poisonKey,
  messages: Array.from({ length: 30 }, (_, i) => ({ seq: i + 1, sender: '甲', userId: '1111111111', text: `毒丸测试消息${i + 1}`, time: 1_000, isSelf: false })),
  now: 10_000
});
const pBatch1 = beginPendingBatch({ memDir: root, key: poisonKey, maxMessages: 30, now: 920_000 });
const afterFail1 = failPendingBatch({ memDir: root, key: poisonKey, batchId: pBatch1.batchId, reason: 'patch-invalid', maxAttempts: 2, now: 925_000 });
const backoffCheck = evaluatePendingTrigger(afterFail1, { ...adaptiveCfg, retryBaseMs: 300_000 }, 1_000_000);
const backoffReady = evaluatePendingTrigger(afterFail1, { ...adaptiveCfg, retryBaseMs: 300_000 }, 1_230_000);
check('失败退避：退避窗口内跳过重试，到期后恢复就绪', backoffCheck.ready === false && backoffCheck.reason === 'retry-backoff' && backoffReady.ready === true && backoffReady.trigger === 'quiet');
const pBatch2 = beginPendingBatch({ memDir: root, key: poisonKey, maxMessages: 30, now: 1_231_000 });
const afterFail2 = failPendingBatch({ memDir: root, key: poisonKey, batchId: pBatch2.batchId, reason: 'patch-invalid', maxAttempts: 2, now: 1_235_000 });
const deadLetters = readDeadLetters(root, poisonKey);
check('死信隔离：达到最大尝试次数自动转入 dead-letter 并清空阻塞', afterFail2.pending.length === 0 && afterFail2.failures.count === 0 && deadLetters.length === 1 && deadLetters[0].count === 30);

// 问答上下文短回复保留 & 引用透传
check(
  '过滤上下文：紧随提问的短确认保留为 normal，独立短确认仍为 low',
  classifyMessage({ userId: '2', text: '对', time: 20_000 }, { previous: { userId: '1', text: '你确定下周搬去杭州吗？', time: 15_000 } }).level === 'normal'
  && classifyMessage({ userId: '2', text: '对', time: 20_000 }).level === 'low'
);
const quoteKey = 'group:616161';
const quoteScan = scanIntoPending({
  memDir: root,
  key: quoteKey,
  messages: [{ seq: 1, sender: '乙', userId: '2222222222', text: '是的', replyTo: { seq: 9, sender: '甲', text: '你养猫吗' }, time: 50_000, isSelf: false }],
  now: 51_000
});
const quotePrompt = buildConsolidationPrompt({ groupName: '引用群', key: quoteKey, messages: quoteScan.queue.pending });
check('引用透传：带 replyTo 的短回复保留并在 prompt 渲染引用上下文', quoteScan.queue.pending.length === 1 && quotePrompt.includes('(回复#9 甲:"你养猫吗") 是的'));

// 旧事实 replace/remove 闭环校验 + 60 条智能淘汰
const promptWithExisting = buildConsolidationPrompt({
  groupName: '测试群',
  key: 'group:1234567890',
  messages: [{ seq: 90, sender: '甲', userId: '1111111111', text: '我现在改喜欢游泳了', time: Date.now(), isSelf: false }],
  memDir: root
});
check(
  '旧事实闭环：整理 prompt 仅注入当前发言人在本作用域内的活跃事实 ID 且不泄露他群事实',
  promptWithExisting.includes('1111111111=>[') && promptWithExisting.includes('global:') && !promptWithExisting.includes('别的群的秘密') && !promptWithExisting.includes('2222222222=>')
);
const absentSpeakerReplace = validatePatchEnvelope(
  { schema: 1, patches: [{ op: 'replace', target: 'member-fact', qq: '2222222222', scope: 'group', groupId: '1234567890', id: 'b1', value: { text: '其实不怕猫了' } }] },
  { memDir: root, key: 'group:1234567890', gid: '1234567890', messages: [{ seq: 90, sender: '甲', userId: '1111111111', text: '小乙其实不怕猫了', time: Date.now(), isSelf: false }] }
);
check('旧事实防篡改：成员未在本批发言时拒绝他人触发的 replace/remove', absentSpeakerReplace.ok === false && absentSpeakerReplace.errors.some((e) => e.includes('未在本批消息发言')));

const evictMsgs = Array.from({ length: 65 }, (_, i) => ({ seq: 200 + i, sender: '容测', userId: '6868686868', text: `事实消息${i}`, time: Date.now() - 1000, isSelf: false }));
const evictPatches = [
  { op: 'add', target: 'member-fact', qq: '6868686868', scope: 'group', groupId: '1234567890', value: { text: '高置信核心职业事实', confidence: 0.98, sourceSeq: 200 } },
  ...Array.from({ length: 64 }, (_, i) => ({
    op: 'add',
    target: 'member-fact',
    qq: '6868686868',
    scope: 'group',
    groupId: '1234567890',
    value: { text: `低置信琐碎事实编号${i + 1}`, confidence: 0.45, sourceSeq: 201 + i }
  }))
];
applyConsolidation({
  memDir: root,
  key: 'group:1234567890',
  gid: '1234567890',
  output: parseConsolidationOutput({ schema: 1, patches: evictPatches.slice(0, 35) } && JSON.stringify({ schema: 1, patches: evictPatches.slice(0, 35) })),
  messages: evictMsgs.slice(0, 35),
  now: Date.now() + 40_000
});
applyConsolidation({
  memDir: root,
  key: 'group:1234567890',
  gid: '1234567890',
  output: parseConsolidationOutput(JSON.stringify({ schema: 1, patches: evictPatches.slice(35) })),
  messages: evictMsgs.slice(35),
  now: Date.now() + 45_000
});
const evictMember = JSON.parse(fs.readFileSync(path.join(root, 'members', '6868686868.json'), 'utf8'));
const kept60 = evictMember.groups['1234567890'].facts;
check('智能容量淘汰：超过 60 条时保留早期高置信核心事实', kept60.length === 60 && kept60.some((f) => f.text === '高置信核心职业事实'));

// Intl.Segmenter 单字词 + 泛群反查 + 多轮代词指代 + 相对日期映射
const openCatQuery = buildContext({
  memDir: root,
  key: 'group:1234567890',
  query: '群里谁怕猫',
  intent: 'auto',
  detail: 'brief',
  messages: [{ seq: 300, sender: '甲', userId: '1111111111', text: '群里谁怕猫', time: Date.now(), isSelf: false }]
});
check('泛群反查与分词：不点名提问“群里谁怕猫”能精准召回本群对应成员且不带外群', openCatQuery.text.includes('很怕猫') && !openCatQuery.text.includes('外群人'), openCatQuery.text);

const pronounFollowup = buildContext({
  memDir: root,
  key: 'group:1234567890',
  query: '那他平时喜欢什么',
  intent: 'auto',
  detail: 'brief',
  messages: [
    { seq: 301, sender: '甲', userId: '1111111111', text: '你还记得二号机吗', time: Date.now() - 10_000, isSelf: false },
    { seq: 302, sender: '机器人', userId: '', text: '记得呀', time: Date.now() - 8_000, isSelf: true },
    { seq: 303, sender: '甲', userId: '1111111111', text: '那他平时喜欢什么', time: Date.now() - 2_000, isSelf: false }
  ]
});
check('多轮代词指代：前文提及成员后用“他”追问可召回对应成员事实', pronounFollowup.text.includes('二号机') && pronounFollowup.text.includes('喜欢拼模型'), pronounFollowup.text);

const dateRangeParsed = extractDateRange('11月3号发生了什么', Date.parse('2026-11-25T12:00:00Z'));
const dateAnchoredTimeline = buildContext({
  memDir: root,
  key: 'group:1234567890',
  query: '11月3号发生了什么',
  intent: 'auto',
  detail: 'brief',
  messages,
  now: Date.parse('2026-11-25T12:00:00Z')
});
check('日期锚点：显式日期无词面重合也能命中对应日期时间线', dateRangeParsed?.from === '2026-11-03' && dateAnchoredTimeline.text.includes('迟到'), dateAnchoredTimeline.text);

// 私聊独立 private 作用域 + 动态人设按群/私聊隔离
const badPrivateGlobal = validatePatchEnvelope(
  { schema: 1, patches: [{ op: 'add', target: 'member-fact', qq: '1111111111', scope: 'global', value: { text: '私聊不允许写全局', sourceSeq: 1 } }] },
  { memDir: root, key: 'private:1111111111', messages: [{ seq: 1, sender: '甲', userId: '1111111111', text: '悄悄话', time: Date.now(), isSelf: false }] }
);
check('私聊隔离：拒绝私聊补丁写入 global scope', badPrivateGlobal.ok === false && badPrivateGlobal.errors.some((e) => e.includes('private scope')));

applyConsolidation({
  memDir: root,
  key: 'private:1111111111',
  output: parseConsolidationOutput(JSON.stringify({
    schema: 1,
    patches: [
      { op: 'add', target: 'member-fact', qq: '1111111111', scope: 'private', value: { text: '私聊专属秘密心事', confidence: 0.95, sourceSeq: 1 } },
      { op: 'replace', target: 'persona', value: { tone: '私聊专属温柔语气', catchphrases: ['抱抱'] } }
    ]
  })),
  messages: [{ seq: 1, sender: '甲', userId: '1111111111', text: '跟你说个心事', time: Date.now(), isSelf: false }],
  now: Date.now() + 50_000
});
applyConsolidation({
  memDir: root,
  key: 'group:1234567890',
  gid: '1234567890',
  output: parseConsolidationOutput(JSON.stringify({
    schema: 1,
    patches: [{ op: 'replace', target: 'persona', value: { tone: '测试群专属沙雕语气', catchphrases: ['芜湖'] } }]
  })),
  messages: [{ seq: 400, sender: '甲', userId: '1111111111', text: '哈哈哈冲', time: Date.now(), isSelf: false }],
  now: Date.now() + 55_000
});
const dmRecall = buildContext({
  memDir: root,
  key: 'private:1111111111',
  query: '你还记得我的心事吗',
  intent: 'general',
  detail: 'full',
  messages: [{ seq: 2, sender: '甲', userId: '1111111111', text: '你还记得我的心事吗', time: Date.now(), isSelf: false }]
});
const group123AfterPrivate = buildContext({
  memDir: root,
  key: 'group:1234567890',
  query: '近况',
  intent: 'general',
  detail: 'full',
  messages: [{ seq: 401, sender: '甲', userId: '1111111111', text: '近况', time: Date.now(), isSelf: false }]
});
const group888PersonaCheck = buildContext({
  memDir: root,
  key: 'group:888',
  query: '近况',
  intent: 'general',
  detail: 'full',
  messages: [{ seq: 20, sender: '外群人', userId: '3333333333', text: '近况', time: Date.now(), isSelf: false }]
});
check('私聊召回：私聊可读本人 private 事实与私聊专属人设', dmRecall.text.includes('私聊专属秘密心事') && dmRecall.text.includes('私聊专属温柔语气'), dmRecall.text);
check('作用域强隔离：群聊绝不泄露私聊 private 事实与私聊人设', !group123AfterPrivate.text.includes('私聊专属秘密心事') && !group123AfterPrivate.text.includes('私聊专属温柔语气') && group123AfterPrivate.text.includes('测试群专属沙雕语气'));
check('动态人设隔离：A 群动态人设不会污染 B 群（B 群回退全局默认人设）', !group888PersonaCheck.text.includes('测试群专属沙雕语气') && group888PersonaCheck.text.includes('简短'));

// 单轮快照复用与整理会话池按会话隔离
const snap = loadSocialSnapshot(path.join(root, 'bridge', 'state', 'social-v2.json'));
check('单轮快照：一次读取同时提供 sig、会话列表与内存消息读取', Boolean(snap.sig) && snap.items.length === 1 && readMessages(snap.conversations, 'group:1234567890').messages.length === 3);
const legacyFlatPool = normalizeConsolidationPool({ sessionId: 'legacy-unscoped', preset: 'qq-chat', baseUrl: 'http://127.0.0.1:9999' });
check('会话池：丢弃未绑定会话 key 的旧全局会话防止跨群复用', Object.keys(legacyFlatPool.sessions).length === 0);
let poolState = normalizeConsolidationPool(null);
poolState = setScopedSession(poolState, 'group:1234567890', { sessionId: 's-g1', preset: 'qq-chat', baseUrl: 'http://127.0.0.1:9999', updatedAt: 100 }, { maxSessions: 2 }).pool;
poolState = setScopedSession(poolState, 'group:888', { sessionId: 's-g2', preset: 'qq-chat', baseUrl: 'http://127.0.0.1:9999', updatedAt: 200 }, { maxSessions: 2 }).pool;
check(
  '会话池：不同群严格隔离各自整理会话 ID',
  getScopedSession(poolState, { key: 'group:1234567890', preset: 'qq-chat', baseUrl: 'http://127.0.0.1:9999' })?.sessionId === 's-g1'
  && getScopedSession(poolState, { key: 'group:888', preset: 'qq-chat', baseUrl: 'http://127.0.0.1:9999' })?.sessionId === 's-g2'
  && getScopedSession(poolState, { key: 'private:1111111111', preset: 'qq-chat', baseUrl: 'http://127.0.0.1:9999' }) === null
);
const evictedPool = setScopedSession(poolState, 'private:1111111111', { sessionId: 's-p1', preset: 'qq-chat', baseUrl: 'http://127.0.0.1:9999', updatedAt: 300 }, { maxSessions: 2 });
check('会话池：超出容量上限按最旧活跃时间 LRU 淘汰', evictedPool.evicted.length === 1 && evictedPool.evicted[0].sessionId === 's-g1');

fs.rmSync(root, { recursive: true, force: true });

console.log(`\n结果：${pass} 通过，${fail} 失败`);
if (fail > 0) process.exitCode = 1;
