import { parseSSEChunk } from "@/lib/sse-parser";
import type { ChatMessage, SSEEvent } from "@/types/chat";

const API_BASE =
  typeof process !== "undefined"
    ? process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080"
    : "http://localhost:8080";

export type StreamChatResult = {
  authRequired?: boolean;
  pipelineId?: string;
};

export type ChatListItem = {
  id: string;
  name: string | null;
  created_at: string | null;
  pipeline_id: string | null;
};

/**
 * GET list of chats. Returns { chats: ChatListItem[] }. Requires auth.
 */
export async function getChatsList(): Promise<{ chats: ChatListItem[] }> {
  const res = await fetch(`${API_BASE}/chat/list`, {
    credentials: "include",
  });
  if (!res.ok) {
    return { chats: [] };
  }
  const json = await res.json();
  const list = Array.isArray(json.chats) ? json.chats : [];
  return { chats: list as ChatListItem[] };
}

/**
 * GET chat_id for a pipeline. Returns { chat_id } or { chat_id: null } when not found / error.
 */
export async function getChatByPipeline(
  pipelineId: string
): Promise<{ chat_id: string | null }> {
  const res = await fetch(`${API_BASE}/chat/by-pipeline/${pipelineId}`, {
    credentials: "include",
  });
  if (!res.ok) {
    return { chat_id: null };
  }
  const json = await res.json();
  return { chat_id: json.chat_id ?? null };
}

/**
 * GET chat history for a chat. Returns { messages }. Returns empty array on error.
 */
export async function getChatHistory(
  chatId: string
): Promise<{ messages: ChatMessage[] }> {
  const res = await fetch(`${API_BASE}/chat/history/${chatId}`, {
    credentials: "include",
  });
  if (!res.ok) {
    return { messages: [] };
  }
  const json = await res.json();
  const list = Array.isArray(json.messages) ? json.messages : [];
  return { messages: list as ChatMessage[] };
}

/**
 * POST to stream endpoint, read SSE stream, call onEvent for each event.
 * Resolves with { authRequired } on 401/403, or { pipelineId } when final event has pipeline_id.
 */
export async function streamChat(params: {
  message: string;
  chatId: string | null;
  onEvent: (evt: SSEEvent) => void;
}): Promise<StreamChatResult> {
  const { message, chatId, onEvent } = params;
  const res = await fetch("/api/chat/stream", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, message, fast: false }),
  });

  if (!res.ok) {
    if (res.status === 401 || res.status === 403) {
      return { authRequired: true };
    }
    return {};
  }

  if (!res.body) {
    return {};
  }

  let pipelineId: string | undefined;
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split("\n\n");
      buffer = parts.pop() ?? "";
      for (const part of parts) {
        const evt = parseSSEChunk(part);
        if (evt) {
          onEvent(evt);
          if (evt.event === "final") {
            const data = evt.data as { pipeline_id?: string };
            if (data.pipeline_id) {
              pipelineId = data.pipeline_id;
            }
          }
        }
      }
    }
  } finally {
    reader.releaseLock();
  }

  return pipelineId ? { pipelineId } : {};
}
