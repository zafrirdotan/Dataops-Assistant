"use client";

import { ScrollArea } from "@/components/ui/scroll-area";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/hooks/useAuth";
import Link from "next/link";
import { Loader2, ChevronDown, ChevronRight, LogOut, User } from "lucide-react";
import { ChatsList } from "@/components/chats-list";
import { ThemeToggle } from "@/components/theme-toggle";
import { PipelinesList } from "@/components/pipelines-list";
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
    const { user, loading: authLoading, logout } = useAuth();
    const isAuthenticated = !!user;
    const [chatsCollapsed, setChatsCollapsed] = useState(false);
    const [pipelinesCollapsed, setPipelinesCollapsed] = useState(false);

    if (authLoading) {
        return (
            <nav className="flex h-full flex-col bg-sidebar">
                <div className="px-3 py-3 flex-shrink-0">
                    <h1 className="text-lg font-semibold text-sidebar-foreground">DataOps Assistant</h1>
                    <p className="text-xs text-muted-foreground">Streaming ETL assistant with live pipeline steps.</p>
                </div>
                <div className="border-t border-sidebar-border my-2" />
                <div className="flex flex-1 items-center justify-center min-h-0">
                    <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                </div>
                <div className="border-t border-sidebar-border my-2 flex-shrink-0" />
                <div className="px-2 py-2 flex-shrink-0" />
            </nav>
        );
    }

    const ChatsHeader = () => (
        <button
            type="button"
            onClick={() => setChatsCollapsed((c) => !c)}
            className="flex w-full items-center gap-1 rounded-md px-2 py-1.5 text-left text-sm font-medium text-sidebar-foreground tracking-tight hover:bg-sidebar-accent transition-colors"
            aria-expanded={!chatsCollapsed}
        >
            {chatsCollapsed ? (
                <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
            ) : (
                <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
            )}
            <span>Chats</span>
        </button>
    );

    const PipelinesHeader = () => (
        <button
            type="button"
            onClick={() => setPipelinesCollapsed((c) => !c)}
            className="flex w-full items-center gap-1 rounded-md px-2 py-1.5 text-left text-sm font-medium text-sidebar-foreground tracking-tight hover:bg-sidebar-accent transition-colors"
            aria-expanded={!pipelinesCollapsed}
        >
            {pipelinesCollapsed ? (
                <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
            ) : (
                <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
            )}
            <span>Pipelines</span>
        </button>
    );

    return (
        <nav className="flex h-full flex-col bg-sidebar">
            <div className="px-3 py-3 flex-shrink-0">
                <h1 className="text-lg font-semibold text-sidebar-foreground">DataOps Assistant</h1>
                <p className="text-xs text-muted-foreground">Streaming ETL assistant with live pipeline steps.</p>
            </div>
            <div className="border-t border-sidebar-border my-2 flex-shrink-0" />

            <div className="flex-1 min-h-0 flex flex-col overflow-hidden">
                <div className="px-2 py-2 flex-shrink-0">
                    <div className="flex items-center justify-between gap-2">
                        <ChatsHeader />
                        {isAuthenticated && (
                            <Button
                                size="sm"
                                variant="ghost"
                                onClick={() => onNewChat?.()}
                                className="h-7 px-2 text-xs text-muted-foreground hover:text-sidebar-foreground hover:bg-sidebar-accent"
                            >
                                New
                            </Button>
                        )}
                    </div>
                </div>
                {!chatsCollapsed && (
                    <ScrollArea>
                        <ChatsList refreshChatsTrigger={refreshChatsTrigger} />
                    </ScrollArea>
                )}
                <div className="px-2 py-2 flex-shrink-0">
                    <PipelinesHeader />
                </div>
                {!pipelinesCollapsed && (
                    <ScrollArea>
                        <PipelinesList />
                    </ScrollArea>
                )}
            </div>

            <div className="border-t border-sidebar-border my-2 flex-shrink-0" />
            <div className="px-2 py-2 flex-shrink-0 flex flex-col gap-1">
                <ThemeToggle />
                {isAuthenticated && (
                    <>
                        <Link href="/profile">
                            <Button
                                variant="ghost"
                                size="sm"
                                className="w-full justify-start gap-2 border-0 shadow-none bg-transparent hover:bg-sidebar-accent text-sidebar-foreground"
                            >
                                <User size={16} />
                                {user?.username}
                            </Button>
                        </Link>
                        <Button
                            variant="ghost"
                            size="sm"
                            onClick={logout}
                            className="w-full justify-start gap-2 border-0 shadow-none bg-transparent hover:bg-sidebar-accent text-sidebar-foreground"
                        >
                            <LogOut size={16} />
                            Logout
                        </Button>
                    </>
                )}
            </div>
        </nav>
    );
}
