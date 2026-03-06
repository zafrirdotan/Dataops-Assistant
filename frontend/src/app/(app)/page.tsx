"use client";

import { useCallback, useEffect } from "react";
import { useRouter, usePathname } from "next/navigation";
import { ChatView } from "@/components/chat-view";
import { useSidebar } from "@/contexts/sidebar-context";

export default function Home() {
  const { setOnNewChat } = useSidebar();
  const router = useRouter();
  const pathname = usePathname();

  const handleNewChat = useCallback(() => {
    router.replace(pathname ?? "/");
  }, [router, pathname]);

  useEffect(() => {
    setOnNewChat(handleNewChat);
  }, [handleNewChat, setOnNewChat]);

  return <ChatView chatId={null} pipelineParam={null} />;
}
