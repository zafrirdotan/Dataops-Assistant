"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { ScrollArea } from "@/components/ui/scroll-area";
import { PipelineSteps } from "@/components/pipeline-steps";
import { PipelineCode } from "@/components/pipeline-code";
import { ChatInput, ChatInputHandle } from "@/components/chat-input";
import { PipelineExamples } from "@/components/pipeline-examples";
import { AuthDialog } from "@/components/auth-dialog";
import { usePipelineContext } from "@/contexts/pipeline-context";
import { useSidebar } from "@/contexts/sidebar-context";
import { getChatHistory } from "@/lib/chat-api";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";
import { ChatMessage, SSEEvent, StepEvent } from "@/types/chat";
import { ResizablePipelinePanels } from "@/components/resizable-pipeline-panels";
import { Loader2 } from "lucide-react";

type ChatViewProps = {
  /** When null, shows "new chat" empty state; when set, loads and shows that chat */
  chatId: string | null;
  /** Optional pipeline context (e.g. from URL) */
  pipelineParam?: string | null;
};

export function ChatView({ chatId: chatIdProp, pipelineParam }: ChatViewProps) {
  const { refreshPipelines } = usePipelineContext();
  const { incrementRefreshChatsTrigger } = useSidebar();
  const router = useRouter();
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [steps, setSteps] = useState<StepEvent[]>([]);
  const [pipelineId, setPipelineId] = useState<string | null>(pipelineParam ?? null);
  const [pipelineCode, setPipelineCode] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [loadingHistory, setLoadingHistory] = useState(!!chatIdProp);
  const [finalError, setFinalError] = useState<string | null>(null);
  const [showAuthDialog, setShowAuthDialog] = useState(false);
  const [chatId, setChatId] = useState<string | null>(chatIdProp);
  const chatInputRef = useRef<ChatInputHandle>(null);
  const scrollEndRef = useRef<HTMLDivElement>(null);

  // Load chat history when chatId is set (e.g. from /c/[chatId] route)
  useEffect(() => {
    if (!chatIdProp) {
      setChatId(null);
      setMessages([]);
      setSteps([]);
      setPipelineCode(null);
      setPipelineId(pipelineParam ?? null);
      setLoadingHistory(false);
      return;
    }
    let cancelled = false;
    setLoadingHistory(true);
    setMessages([]);
    setSteps([]);
    setPipelineCode(null);
    setFinalError(null);
    setChatId(chatIdProp);
    getChatHistory(chatIdProp)
      .then(({ messages: historyMessages }) => {
        if (cancelled) return;
        if (Array.isArray(historyMessages) && historyMessages.length > 0) {
          setMessages(historyMessages);
        }
      })
      .catch((error) => {
        if (cancelled) return;
        console.error("Failed to load chat history:", error);
        setMessages([
          {
            role: "system",
            content: "Failed to load chat history. Please try again.",
          },
        ]);
      })
      .finally(() => {
        if (!cancelled) setLoadingHistory(false);
      });
    return () => {
      cancelled = true;
    };
  }, [chatIdProp, pipelineParam]);

  useEffect(() => {
    scrollEndRef.current?.scrollIntoView({ behavior: "auto" });
  }, [messages, steps, pipelineCode, finalError]);

  const parseSSEChunk = useCallback((chunk: string): SSEEvent | null => {
    let eventName = "message";
    let data = "";
    const lines = chunk.split("\n").filter(Boolean);
    for (const line of lines) {
      if (line.startsWith("event:")) eventName = line.slice(6).trim();
      if (line.startsWith("data:")) data += line.slice(5).trim();
    }
    if (!data) return null;
    try {
      return { event: eventName, data: JSON.parse(data) };
    } catch {
      return null;
    }
  }, []);

  const handleEvent = useCallback(
    (evt: SSEEvent) => {
      if (evt.event === "chat_created") {
        const data = evt.data as { chat_id?: string; name?: string };
        if (data.chat_id) {
          setChatId(data.chat_id);
          // New chat flow: redirect to /c/[id] so URL reflects the chat
          if (!chatIdProp) {
            incrementRefreshChatsTrigger();
            router.replace(`/c/${data.chat_id}`);
            return;
          }
        }
        incrementRefreshChatsTrigger();
        return;
      }
      if (evt.event === "chat_name_updated") {
        incrementRefreshChatsTrigger();
        return;
      }
      if (evt.event === "step") {
        const stepData = evt.data as StepEvent;
        setSteps((prev) => {
          const idx = prev.findIndex((s) => s.step_name === stepData.step_name);
          if (idx === -1) return [...prev, stepData];
          const next = [...prev];
          next[idx] = { ...next[idx], ...stepData };
          return next;
        });
        return;
      }
      if (evt.event === "llm") {
        const delta = String((evt.data as { delta?: string }).delta ?? "");
        if (!delta) return;
        setMessages((prev) => {
          const last = prev[prev.length - 1];
          if (last?.role === "assistant") {
            const next = [...prev];
            next[next.length - 1] = { ...last, content: last.content + delta };
            return next;
          }
          return [...prev, { role: "assistant", content: delta }];
        });
        return;
      }
      if (evt.event === "final") {
        const data = evt.data as {
          pipeline_id?: string;
          pipeline_code?: unknown;
          success?: boolean;
          error?: string;
        };
        setPipelineId(data.pipeline_id ?? null);
        if (data.pipeline_code === undefined || data.pipeline_code === null) {
          setPipelineCode(null);
        } else if (typeof data.pipeline_code === "string") {
          setPipelineCode(data.pipeline_code);
        } else {
          setPipelineCode(JSON.stringify(data.pipeline_code, null, 2));
        }
        if (data.success && data.pipeline_id) {
          refreshPipelines();
        }
        if (data.error) {
          if (data.error.includes("Pipeline tests failed")) {
            setFinalError(data.error);
          } else {
            setMessages((prev) => [
              ...prev,
              { role: "system", content: data.error ?? "" },
            ]);
          }
        }
      }
    },
    [
      chatIdProp,
      router,
      incrementRefreshChatsTrigger,
      refreshPipelines,
    ]
  );

  const handleSubmit = useCallback(async () => {
    const trimmed = input.trim();
    if (!trimmed || isLoading) return;

    setMessages((prev) => [...prev, { role: "user", content: trimmed }]);
    setInput("");
    setSteps([]);
    setPipelineId(null);
    setPipelineCode(null);
    setFinalError(null);
    setIsLoading(true);

    try {
      const res = await fetch("/api/chat/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: chatId,
          message: trimmed,
          fast: false,
        }),
      });

      if (!res.ok || !res.body) {
        if (res.status === 401 || res.status === 403) {
          setShowAuthDialog(true);
          setMessages((prev) => [
            ...prev,
            {
              role: "system",
              content: "Authentication required. Please login to continue.",
            },
          ]);
          return;
        }
        const text = await res.text();
        setMessages((prev) => [
          ...prev,
          { role: "system", content: text || "Request failed" },
        ]);
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() ?? "";
        for (const part of parts) {
          const evt = parseSSEChunk(part);
          if (evt) handleEvent(evt);
        }
      }
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        {
          role: "system",
          content: error instanceof Error ? error.message : "Stream error",
        },
      ]);
    } finally {
      setIsLoading(false);
    }
  }, [input, isLoading, chatId, parseSSEChunk, handleEvent]);

  const hasAssistantMessage = messages.some((m) => m.role === "assistant");
  const showSteps = steps.length > 0;
  const hasStepInProgress = steps.some((s) => s.status === "started");
  const disableSend = isLoading || hasStepInProgress || loadingHistory;

  return (
    <>
      <div className="flex-1 min-h-0 flex flex-col overflow-hidden">
        {messages.length === 0 && loadingHistory ? (
          <div className="flex-1 flex flex-col items-center justify-center px-6">
            <Loader2 className="h-8 w-8 animate-spin text-zinc-400 mb-3" />
            <p className="text-sm text-zinc-500">Loading chat...</p>
          </div>
        ) : messages.length === 0 && !isLoading ? (
          <div className="flex-1 flex flex-col items-center justify-center px-6">
            <div className="w-full max-w-[75%]">
              <h2 className="mb-3 text-center text-3xl font-semibold py-10">
                Describe the pipeline
              </h2>
              <ChatInput
                ref={chatInputRef}
                value={input}
                onChange={setInput}
                onSubmit={handleSubmit}
                disabled={disableSend}
              />
              <PipelineExamples
                onSelectExample={setInput}
                onFocus={() => chatInputRef.current?.focus()}
              />
            </div>
          </div>
        ) : (
          <>
            <ScrollArea className="flex-1 min-h-0 bg-white p-4 overflow-auto">
              <div className="px-2 flex flex-col gap-4">
                {messages.map((message, idx) => {
                  const isStepsOnly =
                    message.role === "system" &&
                    message.extra_data?.type === "steps" &&
                    !message.content?.trim();
                  const msgSteps = message.extra_data?.steps ?? [];
                  const msgCode = message.extra_data?.pipeline_code;
                  const normalizedCode =
                    msgCode == null
                      ? null
                      : typeof msgCode === "string"
                        ? msgCode
                        : JSON.stringify(msgCode, null, 2);
                  const hasStepsOrCode =
                    msgSteps.length > 0 || normalizedCode;

                  if (
                    message.role === "user" ||
                    message.role === "assistant" ||
                    message.role === "system"
                  ) {
                    return (
                      <div
                        key={`${message.role}-${idx}`}
                        className="flex flex-col gap-3"
                      >
                        {!isStepsOnly && (
                          <div
                            className={`flex justify-start ${message.role === "user" ? "" : "pl-4"}`}
                          >
                            <div
                              className={`max-w-[85%] rounded-lg px-4 py-2 text-sm leading-6 ${
                                message.role === "user"
                                  ? "bg-black text-white"
                                  : message.role === "system"
                                    ? "border border-black bg-white text-black"
                                    : "bg-zinc-100 text-black"
                              }`}
                            >
                              <ReactMarkdown
                                remarkPlugins={[remarkGfm]}
                                rehypePlugins={[rehypeHighlight]}
                                components={{
                                  pre: ({ children }) => (
                                    <pre className="overflow-auto rounded-md bg-zinc-900 p-3 text-zinc-100">
                                      {children}
                                    </pre>
                                  ),
                                  code: ({ className, children }) => (
                                    <code className={className ?? ""}>
                                      {children}
                                    </code>
                                  ),
                                }}
                              >
                                {message.content}
                              </ReactMarkdown>
                            </div>
                          </div>
                        )}
                        {hasStepsOrCode && (
                          <div className="flex justify-start pl-4">
                            <div className="w-full max-w-[85%]">
                              <ResizablePipelinePanels
                                left={
                                  msgSteps.length > 0 ? (
                                    <>
                                      <div className="text-xs font-semibold text-zinc-500">
                                        Pipeline steps
                                      </div>
                                      <div className="mt-3">
                                        <PipelineSteps steps={msgSteps} />
                                      </div>
                                    </>
                                  ) : undefined
                                }
                                right={
                                  normalizedCode ? (
                                    <PipelineCode code={normalizedCode} />
                                  ) : undefined
                                }
                              />
                            </div>
                          </div>
                        )}
                      </div>
                    );
                  }
                  return null;
                })}
                {isLoading && !hasAssistantMessage && (
                  <div className="flex justify-start">
                    <div className="max-w-[85%] rounded-lg bg-zinc-100 px-4 py-2 text-sm text-black">
                      Starting work...
                    </div>
                  </div>
                )}
                {(showSteps || pipelineCode) && (
                  <div className="flex justify-start pl-4">
                    <div className="w-full max-w-[85%]">
                      <ResizablePipelinePanels
                        left={
                          showSteps ? (
                            <>
                              <div className="text-xs font-semibold text-zinc-500">
                                Pipeline steps
                              </div>
                              <div className="mt-3">
                                <PipelineSteps steps={steps} />
                              </div>
                            </>
                          ) : undefined
                        }
                        right={
                          pipelineCode ? (
                            <PipelineCode code={pipelineCode} />
                          ) : undefined
                        }
                      />
                    </div>
                  </div>
                )}
                {finalError && (
                  <div className="flex justify-start">
                    <div className="max-w-[85%] rounded-lg border border-black bg-white px-4 py-2 text-sm text-black">
                      {finalError}
                    </div>
                  </div>
                )}
                <div ref={scrollEndRef} />
              </div>
            </ScrollArea>
            <div className="flex-shrink-0 bg-white py-2 border-t border-zinc-200">
              <div className="w-[75%] max-w-[75%] mx-auto px-2">
                <ChatInput
                  value={input}
                  onChange={setInput}
                  onSubmit={handleSubmit}
                  disabled={disableSend}
                  showStatus
                  statusText={isLoading ? "Streaming..." : "Ready"}
                />
              </div>
            </div>
          </>
        )}
      </div>
      <AuthDialog open={showAuthDialog} onOpenChange={setShowAuthDialog} />
    </>
  );
}
