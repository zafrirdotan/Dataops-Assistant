"use client";

import { ScrollArea } from "@/components/ui/scroll-area";
import { Button } from "@/components/ui/button";
import { usePipelineContext } from "@/contexts/pipeline-context";
import { useAuth } from "@/hooks/useAuth";
import { useRouter, usePathname, useSearchParams } from "next/navigation";
import { Loader2, AlertCircle } from "lucide-react";
import { useState, useEffect, useCallback } from "react";
import { getChatsList } from "@/lib/chat-api";
import type { ChatListItem } from "@/lib/chat-api";

const examplePipelines = [
    {
        pipeline_id: "example-1",
        name: "Daily Orders Ingest",
        description: "Automated daily order processing and storage",
        status: "deployed",
        created_at: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
        spec: { schedule: "0 0 * * *" },
    },
    {
        pipeline_id: "example-2",
        name: "Customer Sync",
        description: "Sync customer data from multiple sources",
        status: "deployed",
        created_at: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(),
        spec: { schedule: "0 */6 * * *" },
    },
    {
        pipeline_id: "example-3",
        name: "Payments Snapshot",
        description: "Hourly payment transaction snapshots",
        status: "draft",
        created_at: new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString(),
        spec: { schedule: "0 * * * *" },
    },
];

type PipelineNavProps = {
    onPipelineSelect?: (pipelineId: string) => void;
    onNewChat?: () => void;
    selectedPipelineId?: string | null;
    /** Increment to refetch chats list (e.g. after creating a new chat) */
    refreshChatsTrigger?: number;
};

export function PipelineNav({
    onPipelineSelect,
    onNewChat,
    selectedPipelineId,
    refreshChatsTrigger = 0,
}: PipelineNavProps) {
    const { pipelines, loading, error, refreshPipelines } = usePipelineContext();
    const { user, loading: authLoading } = useAuth();
    const router = useRouter();
    const pathname = usePathname();
    const searchParams = useSearchParams();
    const [showAuthPrompt, setShowAuthPrompt] = useState(false);
    const [chats, setChats] = useState<ChatListItem[]>([]);
    const [chatsLoading, setChatsLoading] = useState(false);

    const selectedChatId = searchParams.get("chat");
    const isAuthenticated = !!user;
    const displayPipelines = isAuthenticated ? pipelines : examplePipelines;

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
        const params = new URLSearchParams(searchParams.toString());
        params.set("chat", chat.id);
        if (chat.pipeline_id) {
            params.set("pipeline", chat.pipeline_id);
        } else {
            params.delete("pipeline");
        }
        router.replace(`${pathname ?? "/"}?${params.toString()}`);
    };

    const formatRelativeTime = (dateString: string) => {
        const date = new Date(dateString);
        const now = new Date();
        const diffInMs = now.getTime() - date.getTime();
        const diffInDays = Math.floor(diffInMs / (1000 * 60 * 60 * 24));

        if (diffInDays === 0) return "Today";
        if (diffInDays === 1) return "Yesterday";
        if (diffInDays < 7) return `${diffInDays} days ago`;
        if (diffInDays < 30) return `${Math.floor(diffInDays / 7)} weeks ago`;
        return `${Math.floor(diffInDays / 30)} months ago`;
    };

    const handlePipelineClick = (pipelineId: string, isExample: boolean) => {
        if (isExample) {
            // Show auth prompt for example pipelines
            setShowAuthPrompt(true);
            router.push("/login");
            return;
        }
        onPipelineSelect?.(pipelineId);
    };

    if (authLoading || loading) {
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

    if (error && isAuthenticated) {
        return (
            <nav className="flex h-full flex-col bg-zinc-50/80">
                <div className="px-4 py-4 flex-shrink-0">
                    <h3 className="text-sm font-medium text-zinc-700 tracking-tight">Chats</h3>
                </div>
                <div className="flex flex-1 flex-col items-center justify-center px-4">
                    <AlertCircle className="h-8 w-8 text-zinc-400 mb-2" />
                    <p className="text-sm text-zinc-600 text-center mb-3">{error}</p>
                    <Button size="sm" onClick={refreshPipelines} variant="ghost" className="text-zinc-600 hover:bg-zinc-200/60">
                        Retry
                    </Button>
                </div>
            </nav>
        );
    }

    return (
        <nav className="flex h-full flex-col bg-zinc-50/80">
            <div className="px-2 py-2 flex-shrink-0">
                <div className="flex items-center justify-between gap-2">
                    <h3 className="text-sm font-medium text-zinc-700 tracking-tight">Chats</h3>
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
            <ScrollArea className="flex-1">
                {isAuthenticated && (
                    <div>
                        {chatsLoading ? (
                            <div className="flex items-center justify-center py-8">
                                <Loader2 className="h-5 w-5 animate-spin text-zinc-400" />
                            </div>
                        ) : chats.length === 0 ? (
                            <div className="px-4 py-4 text-xs text-zinc-500">
                                No chats yet. Start a conversation below.
                            </div>
                        ) : (
                            <ul className="flex flex-col ">
                                {chats.map((chat) => {
                                    const isSelected = selectedChatId === chat.id;
                                    const title = chat.name?.trim() || "New chat";
                                    return (
                                        <li
                                            key={chat.id}
                                            className={`px-3 cursor-pointer transition-colors py-2 ${isSelected
                                                ? "text-zinc-900 bg-zinc-100 rounded-lg "
                                                : "text-zinc-600"
                                                }`}
                                            onClick={() => handleChatClick(chat)}
                                        >
                                            <div className="text-xs font-medium truncate">
                                                {title}
                                            </div>
                                            {/* {chat.created_at && (
                                                <div className={`text-xs mt-0.5 ${isSelected ? "text-zinc-500" : "text-zinc-400"}`}>
                                                    {formatRelativeTime(chat.created_at)}
                                                </div>
                                            )} */}
                                        </li>
                                    );
                                })}
                            </ul>
                        )}
                    </div>
                )}
                {/* <div className="px-4 py-3 border-b border-zinc-200">
          <h3 className="text-xs font-medium text-zinc-500 uppercase tracking-wide">
            Pipelines
          </h3>
        </div>
        {isAuthenticated && pipelines.length === 0 ? (
          <div className="flex flex-col items-center justify-center px-4 py-12 text-center">
            <p className="text-sm font-medium text-zinc-700 mb-1">
              No pipelines yet
            </p>
            <p className="text-xs text-zinc-500">
              Create your first pipeline to get started
            </p>
          </div>
        ) : (
          <ul className="flex flex-col">
            {displayPipelines.map((pipeline) => {
              const isExample = !isAuthenticated;
              const isSelected = selectedPipelineId === pipeline.pipeline_id;
              const schedule = pipeline.spec?.schedule;

              return (
                <li
                  key={pipeline.pipeline_id}
                  className={`px-4 py-3 border-b border-zinc-100 hover:bg-zinc-50 cursor-pointer transition-colors ${
                    isSelected ? "bg-zinc-100 border-l-4 border-l-black" : ""
                  }`}
                  onClick={() =>
                    handlePipelineClick(pipeline.pipeline_id, isExample)
                  }
                >
                  <div className="flex items-start justify-between gap-2 mb-1">
                    <div className="text-sm font-medium text-zinc-900 truncate">
                      {pipeline.name}
                    </div>
                    {isExample && (
                      <Badge variant="secondary" className="text-xs shrink-0">
                        Example
                      </Badge>
                    )}
                  </div>
                  {pipeline.description && (
                    <div className="text-xs text-zinc-600 mb-2 line-clamp-2">
                      {pipeline.description}
                    </div>
                  )}
                  <div className="flex flex-col gap-1 text-xs text-zinc-500">
                    <div className="flex items-center justify-between">
                      <span className="capitalize">{pipeline.status}</span>
                      <span>{formatRelativeTime(pipeline.created_at)}</span>
                    </div>
                    {schedule && (
                      <div className="flex items-center gap-1 text-xs text-zinc-500">
                        <svg
                          className="h-3 w-3"
                          fill="none"
                          stroke="currentColor"
                          viewBox="0 0 24 24"
                        >
                          <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            strokeWidth={2}
                            d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
                          />
                        </svg>
                        <span className="truncate">{schedule}</span>
                      </div>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        )} */}
            </ScrollArea>
            <div className="px-4 py-3 flex-shrink-0 bg-zinc-100/50 rounded-t-xl">
                {!isAuthenticated ? (
                    <div className="text-xs text-zinc-500 text-center">
                        <Button
                            size="sm"
                            variant="link"
                            className="text-xs p-0 h-auto font-medium text-zinc-600 hover:text-zinc-900"
                            onClick={() => router.push("/login")}
                        >
                            Sign in
                        </Button>{" "}
                        or{" "}
                        <Button
                            size="sm"
                            variant="link"
                            className="text-xs p-0 h-auto font-medium text-zinc-600 hover:text-zinc-900"
                            onClick={() => router.push("/signup")}
                        >
                            sign up
                        </Button>{" "}
                        to create your own pipelines
                    </div>
                ) : (
                    <div className="text-xs text-zinc-500 text-center">
                        {pipelines.length} pipeline{pipelines.length !== 1 ? "s" : ""}
                    </div>
                )}
            </div>
        </nav>
    );
}
