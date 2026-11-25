// 消息来源适配层（阶段 1）
//
// 契约：**只读** QQbridge 的 state/social-v2.json 与 state/sessions.json。
// 不写、不锁、不缓存。文件不存在/损坏/正在被原子替换（rename 中间态）时返回空，
// 调用方必须把它们当作"没有新数据"，而不是报错。
//
// 为什么要这一层：QQbridge 是我们的上游，它的内部文件格式不承诺稳定。
// 真出现格式变化时，只改这个文件；而且以后上游若提供标准事件出口，
// 也只需要在这里换实现（保持同样四个导出）。
import fs from 'node:fs';

const readJson = (file) => {
  try {
    let raw = fs.readFileSync(file, 'utf8');
    if (raw.charCodeAt(0) === 0xfeff) raw = raw.slice(1);
    return JSON.parse(raw);
  } catch {
    return null;
  }
};

/** 返回 { conversations, error }；error 非空表示这次没读到，调用方应跳过本轮。 */
export function loadSocialV2(file) {
  const data = readJson(file);
  if (!data || typeof data !== 'object' || !data.conversations || typeof data.conversations !== 'object') {
    return { conversations: {}, error: 'social-v2 不可读或结构异常' };
  }
  return { conversations: data.conversations, error: '' };
}

function parseConversationItems(conversations) {
  const items = [];
  for (const [key, st] of Object.entries(conversations)) {
    const m = /^(group|private):(\d{1,20})$/.exec(key);
    if (!m) continue;
    const messages = Array.isArray(st?.recentMessages) ? st.recentMessages : [];
    items.push({
      key,
      kind: m[1],
      id: m[2],
      lastSeq: Number(messages[messages.length - 1]?.seq) || 0,
      messageCount: messages.length
    });
  }
  return items;
}

/** 一次读取并解析完整快照（含文件签名），供单轮 worker 复用避免重复 JSON.parse。 */
export function loadSocialSnapshot(file) {
  let sig = '';
  try {
    const st = fs.statSync(file, { bigint: true });
    sig = `${st.size}:${st.mtimeNs}`;
  } catch {
    return { conversations: {}, items: [], sig: '', error: 'social-v2 不可读或不存在' };
  }
  const { conversations, error } = loadSocialV2(file);
  if (error) return { conversations: {}, items: [], sig, error };
  return { conversations, items: parseConversationItems(conversations), sig, error: '' };
}

/** [{ key, kind, id, lastSeq, messageCount }]，只列 group: / private: 形式的会话。 */
export function listConversations(file) {
  const { conversations, error } = loadSocialV2(file);
  if (error) return { items: [], error };
  return { items: parseConversationItems(conversations), error: '' };
}

/** 原样返回某会话的 recentMessages（字段透传，不做任何改写）。 */
export function readMessages(fileOrConversations, key) {
  if (fileOrConversations && typeof fileOrConversations === 'object') {
    const st = fileOrConversations[key];
    return { messages: Array.isArray(st?.recentMessages) ? st.recentMessages : [], error: '' };
  }
  const { conversations, error } = loadSocialV2(fileOrConversations);
  if (error) return { messages: [], error };
  const st = conversations[key];
  return { messages: Array.isArray(st?.recentMessages) ? st.recentMessages : [], error: '' };
}

/** 返回 { sessions: { 'group:xxx': 'qqb-g-xxx-…' }, error } */
export function readSessions(file) {
  const data = readJson(file);
  if (!data || typeof data !== 'object') return { sessions: {}, error: 'sessions.json 不可读' };
  return { sessions: data.sessions && typeof data.sessions === 'object' ? data.sessions : {}, error: '' };
}

/** 某会话当前绑定的 DSH 会话 ID（没有就返回 ''）。 */
export function sessionIdOf(file, key) {
  const { sessions } = readSessions(file);
  return String(sessions[key] ?? '');
}
