"use client";

import { useParams, useSearchParams } from "next/navigation";
import { ChatView } from "@/components/chat-view";

export default function ChatPage() {
  const params = useParams();
  const searchParams = useSearchParams();
  const chatId =
    typeof params?.chatId === "string" ? params.chatId : null;
  const pipelineParam = searchParams.get("pipeline");

  if (!chatId) {
    return (
      <div className="flex flex-1 items-center justify-center px-6">
        <p className="text-sm text-zinc-500">Invalid chat</p>
      </div>
    );
  }

  return <ChatView chatId={chatId} pipelineParam={pipelineParam} />;
}
