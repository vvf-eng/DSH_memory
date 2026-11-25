export function usageTotal(usage = {}) {
  return ['cacheMiss', 'cacheHit', 'cacheWrite', 'output', 'reasoning']
    .reduce((sum, key) => sum + (Number(usage?.[key]) || 0), 0);
}

export function nextSessionBudget(state = {}, { steps = 1, usage = {} } = {}) {
  return {
    batches: (Number(state.batches) || 0) + 1,
    steps: (Number(state.steps) || 0) + Math.max(1, Number(steps) || 1),
    tokens: (Number(state.tokens) || 0) + usageTotal(usage)
  };
}

export function sessionRetirementReason(state = {}, limits = {}) {
  if ((Number(state.batches) || 0) >= Math.max(1, Number(limits.maxBatches) || 4)) return 'batches';
  if ((Number(state.steps) || 0) >= Math.max(1, Number(limits.maxSteps) || 12)) return 'steps';
  if ((Number(state.tokens) || 0) >= Math.max(1, Number(limits.maxTokens) || 100_000)) return 'tokens';
  return '';
}

/**
 * 规范化按会话隔离的整理会话池（杜绝不同群/私聊共用同一个整理会话造成上下文串扰）。
 */
export function normalizeConsolidationPool(raw) {
  if (!raw || typeof raw !== 'object') return { schema: 2, sessions: {} };
  if (raw.schema === 2 && raw.sessions && typeof raw.sessions === 'object') {
    return { schema: 2, sessions: { ...raw.sessions } };
  }
  if (raw.sessionId && raw.key) {
    return { schema: 2, sessions: { [String(raw.key)]: { ...raw } } };
  }
  // 旧版未绑定会话 key 的全局会话不再跨群复用，避免把上个群的聊天记录带入下个群。
  return { schema: 2, sessions: {} };
}

export function getScopedSession(pool, { key, preset, baseUrl }) {
  const normalized = normalizeConsolidationPool(pool);
  const entry = normalized.sessions[String(key)];
  if (!entry?.sessionId) return null;
  if (entry.preset !== preset || entry.baseUrl !== baseUrl || entry.key !== String(key)) return null;
  return entry;
}

export function setScopedSession(pool, key, entry, { maxSessions = 8 } = {}) {
  const normalized = normalizeConsolidationPool(pool);
  const targetKey = String(key);
  normalized.sessions[targetKey] = { ...entry, key: targetKey, updatedAt: Number(entry?.updatedAt) || Date.now() };
  const evicted = [];
  const cap = Math.max(1, Number(maxSessions) || 8);
  const keys = Object.keys(normalized.sessions);
  if (keys.length > cap) {
    keys.sort((a, b) => (Number(normalized.sessions[a]?.updatedAt) || 0) - (Number(normalized.sessions[b]?.updatedAt) || 0));
    while (Object.keys(normalized.sessions).length > cap) {
      const oldestKey = keys.shift();
      if (!oldestKey || oldestKey === targetKey) continue;
      evicted.push(normalized.sessions[oldestKey]);
      delete normalized.sessions[oldestKey];
    }
  }
  return { pool: normalized, evicted };
}

export function deleteScopedSession(pool, { key = '', sessionId = '' } = {}) {
  const normalized = normalizeConsolidationPool(pool);
  const removed = [];
  for (const [k, entry] of Object.entries(normalized.sessions)) {
    if ((key && k === key && (!sessionId || entry?.sessionId === sessionId)) || (!key && sessionId && entry?.sessionId === sessionId)) {
      removed.push(entry);
      delete normalized.sessions[k];
    }
  }
  return { pool: normalized, removed };
}
