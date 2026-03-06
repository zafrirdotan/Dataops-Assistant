"use client";

import { useAuth } from "@/hooks/useAuth";
import { useRouter, usePathname } from "next/navigation";
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
  const [chats, setChats] = useState<ChatListItem[]>([]);
  const [chatsLoading, setChatsLoading] = useState(false);

  const selectedChatId =
    pathname?.startsWith("/c/") && pathname !== "/c"
      ? pathname.replace(/^\/c\/?/, "").split("/")[0]
      : null;
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
    const url = chat.pipeline_id
      ? `/c/${chat.id}?pipeline=${encodeURIComponent(chat.pipeline_id)}`
      : `/c/${chat.id}`;
    router.replace(url);
  };

  if (!isAuthenticated) {
    return null;
  }

  if (chatsLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (chats.length === 0) {
    return (
      <div className="px-4 py-4 text-xs text-muted-foreground">
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
            className={`px-3 cursor-pointer transition-colors py-2 rounded-lg ${
              isSelected ? "text-foreground bg-accent" : "text-muted-foreground"
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
