"use client";

import { useAuth } from "@/hooks/useAuth";
import { useRouter, usePathname, useSearchParams } from "next/navigation";
import { Loader2 } from "lucide-react";
import { useState, useEffect, useCallback } from "react";
import { getChatsList } from "@/lib/chat-api";
import type { ChatListItem } from "@/lib/chat-api";

type ChatsListProps = {
  /** Increment to refetch chats list (e.g. after creating a new chat) */
  refreshChatsTrigger?: number;
};

export function ChatsList({ refreshChatsTrigger = 0 }: ChatsListProps) {
  const { user } = useAuth();
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [chats, setChats] = useState<ChatListItem[]>([]);
  const [chatsLoading, setChatsLoading] = useState(false);

  const selectedChatId = searchParams.get("chat");
  const isAuthenticated = !!user;

  const fetchChats = useCallback(async () => {
    if (!isAuthenticated) return;
    setChatsLoading(true);
    try {
      const { chats: list } = await getChatsList();
      setChats(list);
    } catch {
      setChats([]);
    } finally {
      setChatsLoading(false);
    }
  }, [isAuthenticated]);

  useEffect(() => {
    fetchChats();
  }, [fetchChats, refreshChatsTrigger]);

  const handleChatClick = (chat: ChatListItem) => {
    const rawPath = pathname ?? "/";
    const isPipelineDetailPage = rawPath.startsWith("/pipeline/") && rawPath.length > "/pipeline/".length;
    const targetPath = isPipelineDetailPage ? "/" : rawPath;
    const params = new URLSearchParams(searchParams.toString());
    params.set("chat", chat.id);
    if (chat.pipeline_id) {
      params.set("pipeline", chat.pipeline_id);
    } else {
      params.delete("pipeline");
    }
    router.replace(`${targetPath}?${params.toString()}`);
  };

  if (!isAuthenticated) {
    return null;
  }

  if (chatsLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-5 w-5 animate-spin text-zinc-400" />
      </div>
    );
  }

  if (chats.length === 0) {
    return (
      <div className="px-4 py-4 text-xs text-zinc-500">
        No chats yet. Start a conversation below.
      </div>
    );
  }

  return (
    <ul className="flex flex-col">
      {chats.map((chat) => {
        const isSelected = selectedChatId === chat.id;
        const title = chat.name?.trim() || "New chat";
        return (
          <li
            key={chat.id}
            className={`px-3 cursor-pointer transition-colors py-2 ${
              isSelected ? "text-zinc-900 bg-zinc-100 rounded-lg " : "text-zinc-600"
            }`}
            onClick={() => handleChatClick(chat)}
          >
            <div className="text-xs font-medium truncate">{title}</div>
          </li>
        );
      })}
    </ul>
  );
}
