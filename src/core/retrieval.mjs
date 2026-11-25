// v0.10.0 本地记忆检索：先做作用域过滤，再做意图识别、评分、近重复去除和 Top-K。
// 本模块不读写文件，候选必须由 context.mjs 按当前会话范围构造后传入。

export const INTENTS = Object.freeze(['auto', 'member', 'timeline', 'rules', 'relationship', 'general']);

const STOP_WORDS = new Set([
  '这个', '那个', '什么', '怎么', '为什么', '一下', '一个', '我们', '你们', '他们', '她们',
  '是不是', '有没有', '可以', '还是', '就是', '然后', '现在', '今天', '这里', '那里', '知道',
  '群里', '大家', '有人', '哪位', '哪个', '有谁',
  'the', 'and', 'that', 'with', 'this', 'from'
]);

const SINGLE_CHAR_STOP_WORDS = new Set([
  '的', '了', '吗', '么', '呢', '吧', '啊', '呀', '噢', '哦', '嗯', '是', '在', '有', '和', '与',
  '及', '或', '不', '没', '别', '很', '太', '真', '好', '都', '也', '就', '还', '又', '才', '只',
  '要', '会', '能', '可', '把', '被', '给', '让', '对', '向', '往', '从', '到', '去', '来', '做',
  '干', '搞', '弄', '说', '讲', '问', '看', '听', '想', '觉', '得', '着', '过', '我', '你', '他',
  '她', '它', '咱', '您', '谁', '哪', '啥', '某', '这', '那', '个', '位', '些', '点', '种', '次',
  '下', '里', '上', '中', '外', '前', '后', '人', '事', '物', '天', '年', '月', '日', '号'
]);

const ZH_SEGMENTER = typeof Intl?.Segmenter === 'function'
  ? new Intl.Segmenter('zh-CN', { granularity: 'word' })
  : null;

const INTENT_PATTERNS = [
  ['rules', /群规|规矩|忌讳|雷区|能不能|可不可以|允许|禁止|别提|话题|该不该/],
  ['timeline', /上次|之前|以前|过去|当时|后来|发生|哪天|什么时候|最近|还记得|昨天|前天|上周|上个月|\d{1,2}月\d{1,2}[日号]?|为什么.*(?:生气|难过|走|退)/],
  ['relationship', /关系|熟不熟|认识|吵架|闹翻|和好|冷战|站队|喜欢谁|讨厌谁/],
  ['member', /是谁|什么人|喜欢|讨厌|习惯|爱好|职业|工作|性格|生日|住哪|在哪|他|她|这人/]
];

// “还记得我什么”说的是当前人物画像，不是群事件时间线。必须在通用 timeline
// 规则之前判定，否则私聊没有 timeline 候选时会把本人的全部事实过滤掉。
const SELF_MEMORY_MEMBER = /(?:还?记得我(?:什么|哪些|多少|吗)?|对我(?:有)?什么印象|对我的印象|了解我(?:什么|多少)?|关于我(?:知道|记得)?什么|去(?:记忆)?库里(?:翻|查|看)|翻翻(?:记忆)?库)/;

const OLD_ALIAS_MEMBER = /旧称|曾用名|旧名|(?:以前|之前|原来)叫什么/;

export const OPEN_GROUP_MEMBER_RE = /(?:群里|大家|咱们群|本群|这里)?(?:有谁|谁(?:是|有|养|会|懂|在|住|喜欢|讨厌|怕|玩|学|做)|哪(?:位|个群友)|有人.{1,12}吗)/;

export function isOpenGroupMemberQuery(query) {
  const text = String(query ?? '').trim();
  if (!text || /我是谁|我是什么人/.test(text)) return false;
  return OPEN_GROUP_MEMBER_RE.test(text);
}

export function inferIntent(query, requested = 'auto') {
  const explicit = String(requested ?? 'auto').toLowerCase();
  if (INTENTS.includes(explicit) && explicit !== 'auto') return explicit;
  const text = String(query ?? '').trim();
  if (SELF_MEMORY_MEMBER.test(text) || OLD_ALIAS_MEMBER.test(text)) return 'member';
  if (isOpenGroupMemberQuery(text) && !INTENT_PATTERNS[0][1].test(text) && !INTENT_PATTERNS[2][1].test(text)) return 'member';
  for (const [intent, re] of INTENT_PATTERNS) if (re.test(text)) return intent;
  return 'general';
}

export function extractTerms(value) {
  const text = String(value ?? '').toLowerCase()
    .replace(/能不能|可不可以|有没有|是不是|为什么|上次|之前|以前|过去|当时|后来|发生|最近|还记得|群规|规矩|忌讳|雷区|关系|是谁/g, ' ')
    .replace(/\s+/g, ' ');
  const terms = new Set();
  for (const token of text.match(/[a-z0-9_]{2,}/g) ?? []) {
    if (!STOP_WORDS.has(token)) terms.add(token);
  }
  if (ZH_SEGMENTER) {
    for (const seg of ZH_SEGMENTER.segment(text)) {
      if (!seg.isWordLike) continue;
      const word = seg.segment.trim();
      if (!/^[\u3400-\u9fff]+$/.test(word)) continue;
      if (word.length === 1) {
        if (!SINGLE_CHAR_STOP_WORDS.has(word)) terms.add(word);
      } else if (word.length <= 8 && !STOP_WORDS.has(word)) {
        terms.add(word);
      }
    }
  }
  for (const chunk of text.match(/[\u3400-\u9fff]{2,}/g) ?? []) {
    if (!STOP_WORDS.has(chunk) && chunk.length <= 8) terms.add(chunk);
    for (let i = 0; i + 1 < chunk.length; i += 1) {
      const pair = chunk.slice(i, i + 2);
      if (!STOP_WORDS.has(pair)) terms.add(pair);
    }
  }
  return terms;
}

function aliasesOf(member, gid) {
  const group = gid ? member?.groups?.[gid] : null;
  return [
    String(member?.qq ?? ''),
    String(member?.displayName ?? ''),
    ...(Array.isArray(member?.aliases) ? member.aliases.map(String) : []),
    String(group?.alias ?? ''),
    ...(Array.isArray(group?.aliases) ? group.aliases.map(String) : [])
  ].map((s) => s.trim()).filter(Boolean);
}

export function mentionedMemberIds(members, query, gid) {
  const text = String(query ?? '').toLowerCase();
  const found = new Set();
  for (const member of Array.isArray(members) ? members : []) {
    for (const alias of aliasesOf(member, gid)) {
      const normalized = alias.toLowerCase();
      if ((/^\d+$/.test(normalized) || normalized.length >= 2) && text.includes(normalized)) {
        found.add(String(member.qq));
        break;
      }
    }
  }
  return found;
}

function dateMs(value) {
  const n = Date.parse(String(value ?? ''));
  return Number.isFinite(n) ? n : 0;
}

function freshScore(value, now) {
  const at = dateMs(value);
  if (!at) return 0;
  const days = Math.max(0, (now - at) / 86_400_000);
  if (days <= 7) return 15;
  if (days <= 30) return 12;
  if (days <= 180) return 8;
  if (days <= 365) return 4;
  return 0;
}

const pad2 = (n) => String(n).padStart(2, '0');
const fmtDay = (d) => `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;

/** 解析查询中的相对或显式日期区间，辅助时间线与事件定位。 */
export function extractDateRange(query, now = Date.now()) {
  const text = String(query ?? '');
  if (!text) return null;
  const base = new Date(now);
  const shift = (days) => {
    const d = new Date(base.getFullYear(), base.getMonth(), base.getDate() + days);
    return fmtDay(d);
  };
  if (/大前天/.test(text)) { const d = shift(-3); return { from: d, to: d }; }
  if (/前天/.test(text)) { const d = shift(-2); return { from: d, to: d }; }
  if (/昨天|昨日/.test(text)) { const d = shift(-1); return { from: d, to: d }; }
  if (/今天|今日/.test(text)) { const d = shift(0); return { from: d, to: d }; }
  if (/上周|上礼拜|上星期/.test(text)) return { from: shift(-14), to: shift(-4) };
  if (/这周|本周|这礼拜|这星期/.test(text)) return { from: shift(-7), to: shift(0) };
  if (/上个月/.test(text)) {
    const first = new Date(base.getFullYear(), base.getMonth() - 1, 1);
    const last = new Date(base.getFullYear(), base.getMonth(), 0);
    return { from: fmtDay(first), to: fmtDay(last) };
  }
  const exactMd = /(?:(\d{4})年)?(\d{1,2})月(\d{1,2})[日号]?/.exec(text);
  if (exactMd) {
    const year = exactMd[1] ? Number(exactMd[1]) : base.getFullYear();
    const month = Number(exactMd[2]);
    const day = Number(exactMd[3]);
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      const target = `${year}-${pad2(month)}-${pad2(day)}`;
      return { from: target, to: target };
    }
  }
  const monthOnly = /(?:(\d{4})年)?(\d{1,2})月(?:份|初|中|底)?/.exec(text);
  if (monthOnly) {
    const year = monthOnly[1] ? Number(monthOnly[1]) : base.getFullYear();
    const month = Number(monthOnly[2]);
    if (month >= 1 && month <= 12) {
      const first = new Date(year, month - 1, 1);
      const last = new Date(year, month, 0);
      return { from: fmtDay(first), to: fmtDay(last) };
    }
  }
  return null;
}

function buildTermWeights(queryTerms, candidates) {
  const weights = new Map();
  if (!queryTerms.size) return weights;
  const total = Math.max(1, candidates.length);
  const df = new Map();
  for (const c of candidates) {
    const cTerms = c._terms ?? extractTerms(c.text);
    for (const term of queryTerms) {
      if (cTerms.has(term)) df.set(term, (df.get(term) ?? 0) + 1);
    }
  }
  for (const term of queryTerms) {
    const hits = df.get(term) ?? 0;
    const rawIdf = 6 + Math.round(5 * Math.log((total + 1) / (hits + 1)));
    const bounded = Math.max(4, Math.min(14, rawIdf));
    const lenFactor = term.length === 1 ? 0.75 : 1;
    weights.set(term, Math.max(3, Math.round(bounded * lenFactor)));
  }
  return weights;
}

export function overlapScore(queryTerms, text, termWeights = null, precomputedTerms = null) {
  if (!queryTerms.size) return 0;
  const candidateTerms = precomputedTerms ?? extractTerms(text);
  let points = 0;
  for (const term of queryTerms) {
    if (!candidateTerms.has(term)) continue;
    points += termWeights ? (termWeights.get(term) ?? 8) : 8;
  }
  return Math.min(36, points);
}

function bigrams(value) {
  const text = String(value ?? '').toLowerCase().replace(/[\s\p{P}\p{S}]+/gu, '');
  const out = new Set();
  for (let i = 0; i + 1 < text.length; i += 1) out.add(text.slice(i, i + 2));
  return out;
}

export function textSimilarity(a, b) {
  const aa = bigrams(a);
  const bb = bigrams(b);
  if (!aa.size || !bb.size) return String(a).trim() === String(b).trim() ? 1 : 0;
  let common = 0;
  for (const pair of aa) if (bb.has(pair)) common += 1;
  return (2 * common) / (aa.size + bb.size);
}

function activeCandidate(candidate, now) {
  if (!candidate || !String(candidate.text ?? '').trim()) return false;
  if (String(candidate.status ?? 'active') !== 'active') return false;
  if (candidate.supersededBy) return false;
  const expires = dateMs(candidate.expiresAt);
  return !expires || expires > now;
}

function dateMatchesRange(candidate, range) {
  if (!range) return false;
  const day = String(candidate?.date ?? candidate?.createdAt ?? '').slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return false;
  return day >= range.from && day <= range.to;
}

function scoreCandidate(candidate, { query, lexicalScore, intent, speakerId, mentionedIds, contextMentionedIds, dateMatched, openGroupQuery, gid, now }) {
  let score = lexicalScore;
  const memberId = String(candidate.memberId ?? '');
  const askingAboutOthersOnly = !/我/.test(String(query ?? '')) && (
    (mentionedIds.size > 0 && !mentionedIds.has(speakerId))
    || (contextMentionedIds?.size > 0 && !contextMentionedIds.has(speakerId))
  );
  if (memberId && memberId === speakerId && !openGroupQuery && !askingAboutOthersOnly) score += 50;
  if (memberId && mentionedIds.has(memberId)) score += 40;
  else if (memberId && contextMentionedIds?.has(memberId)) score += 38;
  if (candidate.openLookupMatch) score += 32;
  if (dateMatched) score += 25;
  if (candidate.aliases?.some((alias) => String(alias).length >= 2 && String(query).toLowerCase().includes(String(alias).toLowerCase()))) score += 40;
  if (candidate.scope === 'group' && String(candidate.groupId ?? '') === String(gid ?? '')) score += 10;
  if (candidate.scope === 'private') score += 10;
  score += freshScore(candidate.date ?? candidate.createdAt, now);
  const importance = candidate.importance == null ? 0.5 : Number(candidate.importance);
  const confidence = candidate.confidence == null ? 0.5 : Number(candidate.confidence);
  score += Math.round(Math.max(0, Math.min(1, Number.isFinite(importance) ? importance : 0.5)) * 10);
  score += Math.round(Math.max(0, Math.min(1, Number.isFinite(confidence) ? confidence : 0.5)) * 10);

  const typeIntent = {
    member: 'member',
    timeline: 'timeline',
    rule: 'rules',
    relationship: 'relationship'
  }[candidate.type];
  if (intent !== 'general' && typeIntent === intent) score += 30;
  if (intent === 'relationship' && candidate.type === 'member' && candidate.relation) score += 20;
  if (intent === 'general') {
    if (candidate.type === 'member') score += 8;
    if (candidate.type === 'rule') score += 5;
  }
  const ageDays = candidate.date ? (now - dateMs(candidate.date)) / 86_400_000 : 0;
  if (Number(candidate.confidence) > 0 && Number(candidate.confidence) < 0.5 && ageDays > 180) score -= 20;
  return score;
}

const LIMITS = {
  tiny: { member: 2, timeline: 1, rule: 1, relationship: 1, persona: 1, vibe: 1 },
  brief: { member: 5, timeline: 3, rule: 2, relationship: 2, persona: 1, vibe: 1 },
  full: { member: 8, timeline: 5, rule: 2, relationship: 3, persona: 1, vibe: 1 }
};

/**
 * 候选已经过调用方的作用域过滤；这里绝不把被过滤掉的候选重新找回来。
 */
export function rankMemoryCandidates({
  candidates = [],
  query = '',
  intent = 'auto',
  detail = 'brief',
  speakerId = '',
  mentionedIds = new Set(),
  contextMentionedIds = new Set(),
  gid = '',
  now = Date.now()
} = {}) {
  const resolvedIntent = inferIntent(query, intent);
  const queryTerms = extractTerms(query);
  const dateRange = extractDateRange(query, now);
  const openGroupQuery = Boolean(gid) && mentionedIds.size === 0 && isOpenGroupMemberQuery(query);
  const limits = LIMITS[detail] ?? LIMITS.brief;
  const active = candidates
    .filter((candidate) => activeCandidate(candidate, now))
    .map((candidate, order) => ({ ...candidate, _terms: extractTerms(candidate.text), _order: order }));
  const termWeights = buildTermWeights(queryTerms, active);
  const scored = active
    .map((candidate) => {
      const lexical = overlapScore(queryTerms, candidate.text, termWeights, candidate._terms);
      const dateMatched = dateMatchesRange(candidate, dateRange);
      return {
        ...candidate,
        score: scoreCandidate(candidate, {
          query,
          lexicalScore: lexical,
          intent: resolvedIntent,
          speakerId: String(speakerId),
          mentionedIds,
          contextMentionedIds,
          dateMatched,
          openGroupQuery,
          gid,
          now
        }),
        _lexical: lexical,
        _dateMatched: dateMatched
      };
    })
    .sort((a, b) => b.score - a.score || freshScore(b.date, now) - freshScore(a.date, now) || a._order - b._order);

  const counts = new Map();
  const selected = [];
  for (const candidate of scored) {
    const expected = { member: 'member', relationship: 'relationship', timeline: 'timeline', rule: 'rules' }[candidate.type];
    if (resolvedIntent !== 'general' && expected && expected !== resolvedIntent && candidate._lexical === 0 && !candidate._dateMatched) {
      const mid = String(candidate.memberId ?? '');
      const namedPerson = (candidate.type === 'member' || candidate.type === 'relationship')
        && (mentionedIds.has(mid) || contextMentionedIds.has(mid));
      if (!namedPerson) continue;
    }
    if (resolvedIntent !== 'general' && (candidate.type === 'persona' || candidate.type === 'vibe') && candidate._lexical === 0) continue;
    const bucket = candidate.type === 'relationship' ? 'member' : candidate.type;
    if ((counts.get(bucket) ?? 0) >= (limits[bucket] ?? 0)) continue;
    if (selected.some((prior) => prior.type === candidate.type && textSimilarity(prior.text, candidate.text) >= 0.82)) continue;
    selected.push(candidate);
    counts.set(bucket, (counts.get(bucket) ?? 0) + 1);
  }
  return { intent: resolvedIntent, selected };
}
