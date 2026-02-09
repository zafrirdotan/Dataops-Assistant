"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { PipelineNav } from "@/components/pipeline-nav";
import { AppHeader } from "@/components/app-header";
import { PipelineSteps } from "@/components/pipeline-steps";
import { PipelineCode } from "@/components/pipeline-code";
import { ChatInput, ChatInputHandle } from "@/components/chat-input";
import { PipelineExamples } from "@/components/pipeline-examples";
import { AuthDialog } from "@/components/auth-dialog";
import { usePipelineContext } from "@/contexts/pipeline-context";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";

export default function Home() {
  const { refreshPipelines } = usePipelineContext();
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [steps, setSteps] = useState<StepEvent[]>([]);
  const [pipelineId, setPipelineId] = useState<string | null>(null);
  const [pipelineCode, setPipelineCode] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [finalError, setFinalError] = useState<string | null>(null);
  const [showAuthDialog, setShowAuthDialog] = useState(false);
  const [chatId, setChatId] = useState<string | null>(null);
  const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null);
  const chatInputRef = useRef<ChatInputHandle>(null);
  const scrollEndRef = useRef<HTMLDivElement>(null);

  // Load pipeline chat history when a pipeline is selected
  useEffect(() => {
    const loadPipelineHistory = async () => {
      if (!selectedPipeline) return;

      try {
        setLoadingHistory(true);
        setMessages([]);
        setSteps([]);
        setPipelineCode(null);
        setFinalError(null);

        // Fetch chat_id for this pipeline
        const chatIdRes = await fetch(
          `http://localhost:8080/chat/by-pipeline/${selectedPipeline}`,
          {
            credentials: "include",
          },
        );

        if (chatIdRes.ok) {
          const { chat_id } = await chatIdRes.json();
          setChatId(chat_id);

          // Fetch chat history
          if (chat_id) {
            const historyRes = await fetch(
              `http://localhost:8080/chat/history/${chat_id}`,
              {
                credentials: "include",
              },
            );

            if (historyRes.ok) {
              const { messages } = await historyRes.json();

              if (messages.length > 0) {
                setMessages(messages as ChatMessage[]);
              }
            }
          }
        }
      } catch (error) {
        console.error("Failed to load pipeline history:", error);
        setMessages([
          {
            role: "system",
            content: "Failed to load pipeline history. Please try again.",
          },
        ]);
      } finally {
        setLoadingHistory(false);
      }
    };

    loadPipelineHistory();
  }, [selectedPipeline]);

  // Auto-scroll to bottom when new messages, steps, or pipeline data arrive
  useEffect(() => {
    scrollEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, steps, pipelineCode, finalError]);

  const parseSSEChunk = (chunk: string): SSEEvent | null => {
    let eventName = "message";
    let data = "";
    const lines = chunk.split("\n").filter(Boolean);
    for (const line of lines) {
      if (line.startsWith("event:")) {
        eventName = line.slice(6).trim();
      }
      if (line.startsWith("data:")) {
        data += line.slice(5).trim();
      }
    }
    if (!data) return null;
    try {
      return { event: eventName, data: JSON.parse(data) };
    } catch {
      return null;
    }
  };

  const handleEvent = (evt: SSEEvent) => {
    if (evt.event === "chat_created") {
      const data = evt.data as { chat_id?: string };
      if (data.chat_id) {
        setChatId(data.chat_id);
      }
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
          next[next.length - 1] = {
            ...last,
            content: last.content + delta,
          };
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
        setMessages((prev) => [
          ...prev,
          {
            role: "assistant",
            content: `Pipeline ${data.pipeline_id} created successfully.`,
          },
        ]);
        // Refresh pipeline list when a new pipeline is created
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
  };

  const handleSubmit = async () => {
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
      // Send only chat_id and new message - backend fetches full history from DB
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
        // Check if it's an authentication error
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
  };

  const handlePipelineSelect = (pipelineId: string) => {
    setSelectedPipeline(pipelineId);
    setPipelineId(pipelineId);
  };

  const handleNewChat = () => {
    setSelectedPipeline(null);
    setChatId(null);
    setMessages([]);
    setSteps([]);
    setPipelineId(null);
    setPipelineCode(null);
    setFinalError(null);
  };

  const hasAssistantMessage = messages.some(
    (message) => message.role === "assistant",
  );
  const showSteps = steps.length > 0;
  const hasStepInProgress = steps.some((step) => step.status === "started");
  const disableSend = isLoading || hasStepInProgress || loadingHistory;

  return (
    <div className="h-screen overflow-hidden bg-white text-black flex flex-col">
      <AppHeader />
      <div className="flex flex-1 overflow-hidden">
        <aside className="hidden w-72 lg:flex lg:h-full">
          <PipelineNav
            onPipelineSelect={handlePipelineSelect}
            onNewChat={handleNewChat}
            selectedPipelineId={selectedPipeline}
          />
        </aside>

        <main className="flex flex-1 flex-col min-h-0 overflow-hidden">
          <div className="flex flex-1 items-stretch justify-center min-h-0 overflow-hidden px-6">
            <div className="flex w-full max-w-4xl flex-col gap-4 min-h-0 overflow-hidden mx-auto">
              {messages.length === 0 && !isLoading ? (
                <div className="flex flex-1 items-center justify-center w-full mx-auto -mt-50">
                  <div className="w-full">
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
                  <div className="flex flex-1 flex-col min-h-0 overflow-hidden">
                    <ScrollArea className="flex-1 min-h-0 bg-white p-4">
                      <div className="flex flex-col gap-4">
                        {messages.map((message, idx) => {
                          if (
                            message.role === "user" ||
                            message.role === "assistant" ||
                            message.role === "system"
                          ) {
                            return (
                              <div
                                key={`${message.role}-${idx}`}
                                className={`flex ${
                                  message.role === "user"
                                    ? "justify-end"
                                    : "justify-start"
                                }`}
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
                            );
                            if (message.role === "system") {
                              if (message.extra_data?.type === "steps") {
                                return (
                                  <PipelineSteps
                                    steps={
                                      message.extra_data?.steps as StepEvent[]
                                    }
                                  />
                                );
                              }
                              return null;
                            }
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

                        {/* Unified display for steps and code - works for both streaming and history */}
                        {(showSteps || pipelineCode) && (
                          <div className="flex justify-start">
                            <div className="flex w-full flex-wrap gap-4">
                              {showSteps && (
                                <div className="w-[300px] rounded-lg border border-black/10 bg-white px-4 py-3">
                                  <div className="text-xs font-semibold text-zinc-500">
                                    Pipeline steps
                                  </div>
                                  <div className="mt-3">
                                    <PipelineSteps steps={steps} />
                                  </div>
                                </div>
                              )}

                              {pipelineCode && (
                                <div className="min-w-[280px] flex-1">
                                  <PipelineCode code={pipelineCode} />
                                </div>
                              )}
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

                        {/* Scroll anchor */}
                        <div ref={scrollEndRef} />
                      </div>
                    </ScrollArea>

                    <div className="sticky bottom-0 border-t border-black/10 bg-white px-4 py-4">
                      <div className="w-full mx-auto">
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
                  </div>
                </>
              )}
            </div>
          </div>
        </main>
      </div>

      {/* Authentication Dialog */}
      <AuthDialog open={showAuthDialog} onOpenChange={setShowAuthDialog} />
    </div>
  );
}
