"use client";

import { ScrollArea } from "@/components/ui/scroll-area";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/hooks/useAuth";
import { useRouter } from "next/navigation";
import { Loader2 } from "lucide-react";
import { ChatsList } from "@/components/chats-list";

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
        <ChatsList refreshChatsTrigger={refreshChatsTrigger} />
      </ScrollArea>
      <div className="px-4 py-3 flex-shrink-0 bg-zinc-100/50 rounded-t-xl">
        {!isAuthenticated && (
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
        )}
      </div>
    </nav>
  );
}
