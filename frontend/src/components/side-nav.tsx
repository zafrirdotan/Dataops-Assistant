"use client";

import { ScrollArea } from "@/components/ui/scroll-area";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/hooks/useAuth";
import { useRouter } from "next/navigation";
import { Loader2, ChevronDown, ChevronRight } from "lucide-react";
import { ChatsList } from "@/components/chats-list";
import { useState } from "react";

type SideNavProps = {
    onNewChat?: () => void;
    /** Increment to refetch chats list (e.g. after creating a new chat) */
    refreshChatsTrigger?: number;
};

export function SideNav({
    onNewChat,
    refreshChatsTrigger = 0,
}: SideNavProps) {
    const { user, loading: authLoading } = useAuth();
    const router = useRouter();
    const isAuthenticated = !!user;
    const [chatsCollapsed, setChatsCollapsed] = useState(false);

    if (authLoading) {
        return (
            <nav className="flex h-full flex-col bg-zinc-50/80">
                <div className="px-4 py-4 flex-shrink-0">
                    <h3 className="text-sm font-medium text-zinc-700 tracking-tight">Chats</h3>
                </div>
                <div className="flex flex-1 items-center justify-center">
                    <Loader2 className="h-6 w-6 animate-spin text-zinc-400" />
                </div>
            </nav>
        );
    }

    const ChatsHeader = () => (
        <button
            type="button"
            onClick={() => setChatsCollapsed((c) => !c)}
            className="flex w-full items-center gap-1 rounded-md px-2 py-1.5 text-left text-sm font-medium text-zinc-700 tracking-tight hover:bg-zinc-200/60 transition-colors"
            aria-expanded={!chatsCollapsed}
        >
            {chatsCollapsed ? (
                <ChevronRight className="h-4 w-4 shrink-0 text-zinc-500" />
            ) : (
                <ChevronDown className="h-4 w-4 shrink-0 text-zinc-500" />
            )}
            <span>Chats</span>
        </button>
    );

    return (
        <nav className="flex h-full flex-col bg-zinc-50/80">
            <div className="px-2 py-2 flex-shrink-0">
                <div className="flex items-center justify-between gap-2">
                    <ChatsHeader />
                    {isAuthenticated && (
                        <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => onNewChat?.()}
                            className="h-7 px-2 text-xs text-zinc-600 hover:text-zinc-900 hover:bg-zinc-200/60"
                        >
                            New
                        </Button>
                    )}
                </div>
            </div>
            {!chatsCollapsed && (
                <ScrollArea className="flex-1">
                    <ChatsList refreshChatsTrigger={refreshChatsTrigger} />
                </ScrollArea>
            )}

        </nav>
    );
}
