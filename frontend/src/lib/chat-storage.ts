/**
 * Chat persistence utilities for loading chat messages
 *
 * NOTE: Chat messages are automatically saved by the backend during streaming.
 * The frontend only needs to load existing chat history.
 */

export type ChatMessage = {
  role: "user" | "assistant" | "system" | "steps" | "code";
  content: string;
  extra_data?: {
    type?: string;
    pipeline_id?: string;
    pipeline_code?: string;
    steps?: Array<{
      step_name: string;
      step_number: number;
      message: string;
      status: string;
      error?: string;
    }>;
  };
};

export type SavedChatMessage = {
  id: number;
  chat_id: string;
  role: string;
  content: string;
  extra_data?: any;
  created_at: string;
};

/**
 * Load chat history for a specific chat
 * Chat is automatically saved by the backend during streaming
 */
export async function loadChatHistory(chatId: string): Promise<ChatMessage[]> {
  try {
    const token = localStorage.getItem("access_token");
    if (!token) {
      console.error("No access token found");
      return [];
    }

    const response = await fetch(
      `http://localhost:8080/chat/history/${chatId}`,
      {
        method: "GET",
        credentials: "include", // Include cookies for authentication
      },
    );

    if (!response.ok) {
      console.error("Failed to load chat history:", await response.text());
      return [];
    }

    const data = await response.json();
    return data.messages.map((msg: SavedChatMessage) => ({
      role: msg.role as ChatMessage["role"],
      content: msg.content,
      extra_data: msg.extra_data,
    }));
  } catch (error) {
    console.error("Error loading chat history:", error);
    return [];
  }
}
