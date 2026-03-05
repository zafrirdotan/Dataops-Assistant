import type { SSEEvent } from "@/types/chat";

/**
 * Parse a single SSE chunk (double-newline separated) into an event object.
 * Returns null if no data or JSON parse fails.
 */
export function parseSSEChunk(chunk: string): SSEEvent | null {
  let eventName = "message";
  let data = "";
  const lines = chunk.split("\n").filter(Boolean);
  for (const line of lines) {
    if (line.startsWith("event:")) {
      eventName = line.slice(6).trim();
    }
    if (line.startsWith("data:")) {
      data += line.slice(5).trim();
    }
  }
  if (!data) return null;
  try {
    return { event: eventName, data: JSON.parse(data) as Record<string, unknown> };
  } catch {
    return null;
  }
}
