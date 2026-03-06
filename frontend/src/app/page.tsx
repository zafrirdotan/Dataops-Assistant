"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter, usePathname, useSearchParams } from "next/navigation";
import { ScrollArea } from "@/components/ui/scroll-area";
import { PipelineNav } from "@/components/pipeline-nav";
import { AppHeader } from "@/components/app-header";
import { PipelineSteps } from "@/components/pipeline-steps";
import { PipelineCode } from "@/components/pipeline-code";
import { ChatInput, ChatInputHandle } from "@/components/chat-input";
import { PipelineExamples } from "@/components/pipeline-examples";
import { AuthDialog } from "@/components/auth-dialog";
import { usePipelineContext } from "@/contexts/pipeline-context";
import { getChatHistory, getChatByPipeline } from "@/lib/chat-api";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";
import { ChatMessage, SSEEvent, StepEvent } from "@/types/chat";
import {
    ResizablePanelGroup,
    ResizablePanel,
    ResizableHandle,
} from "@/components/ui/resizable";
import { ResizablePipelinePanels } from "@/components/resizable-pipeline-panels";
import { Loader2 } from "lucide-react";

export default function Home() {
    const { refreshPipelines } = usePipelineContext();
    const router = useRouter();
    const pathname = usePathname();
    const searchParams = useSearchParams();
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
    const [refreshChatsTrigger, setRefreshChatsTrigger] = useState(0);
    const chatInputRef = useRef<ChatInputHandle>(null);
    const scrollEndRef = useRef<HTMLDivElement>(null);

    const chatParam = searchParams.get("chat");
    const pipelineParam = searchParams.get("pipeline");

    // Load chat/pipeline from URL (single source of truth)
    useEffect(() => {
        const loadFromUrl = async () => {
            if (chatParam) {
                // Load history by chat ID
                setSelectedPipeline(pipelineParam);
                setPipelineId(pipelineParam);
                try {
                    setLoadingHistory(true);
                    setMessages([]);
                    setSteps([]);
                    setPipelineCode(null);
                    setFinalError(null);
                    const { messages: historyMessages } = await getChatHistory(chatParam);
                    setChatId(chatParam);
                    if (Array.isArray(historyMessages) && historyMessages.length > 0) {
                        setMessages(historyMessages);
                    }
                } catch (error) {
                    console.error("Failed to load chat history:", error);
                    setMessages([
                        {
                            role: "system",
                            content: "Failed to load chat history. Please try again.",
                        },
                    ]);
                } finally {
                    setLoadingHistory(false);
                }
            } else if (pipelineParam) {
                // Load by pipeline
                setSelectedPipeline(pipelineParam);
                setPipelineId(pipelineParam);
                try {
                    setLoadingHistory(true);
                    setMessages([]);
                    setSteps([]);
                    setPipelineCode(null);
                    setFinalError(null);
                    const { chat_id } = await getChatByPipeline(pipelineParam);
                    setChatId(chat_id);
                    if (chat_id) {
                        const { messages: historyMessages } = await getChatHistory(chat_id);
                        if (Array.isArray(historyMessages) && historyMessages.length > 0) {
                            setMessages(historyMessages);
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
            } else {
                setSelectedPipeline(null);
                setPipelineId(null);
                setChatId(null);
                setMessages([]);
                setSteps([]);
                setPipelineCode(null);
                setFinalError(null);
            }
        };

        loadFromUrl();
    }, [chatParam, pipelineParam]);

    // Auto-scroll to bottom when new messages, steps, or pipeline data arrive
    useEffect(() => {
        scrollEndRef.current?.scrollIntoView({ behavior: "auto" });
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
            const data = evt.data as { chat_id?: string; name?: string };
            if (data.chat_id) {
                setChatId(data.chat_id);
            }
            setRefreshChatsTrigger((t) => t + 1);
            return;
        }

        if (evt.event === "chat_name_updated") {
            setRefreshChatsTrigger((t) => t + 1);
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
                // setMessages((prev) => [
                //     ...prev,
                //     {
                //         role: "assistant",
                //         content: `Pipeline ${data.pipeline_id} created successfully.`,
                //     },
                // ]);
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
        const params = new URLSearchParams(searchParams.toString());
        params.set("pipeline", pipelineId);
        params.delete("chat");
        router.replace(`${pathname ?? "/"}?${params.toString()}`);
    };

    const handleNewChat = () => {
        router.replace(pathname ?? "/");
    };

    const hasAssistantMessage = messages.some(
        (message) => message.role === "assistant",
    );
    const showSteps = steps.length > 0;
    const hasStepInProgress = steps.some((step) => step.status === "started");
    const disableSend = isLoading || hasStepInProgress || loadingHistory;

    return (
        <div className="h-screen overflow-hidden bg-white text-black flex flex-col">
            <div className="flex flex-1 min-h-0">
                <ResizablePanelGroup orientation="horizontal" className="flex-1 min-h-0">
                    <ResizablePanel defaultSize="20%" className="hidden lg:block min-w-0">
                        <aside className="h-full w-full">
                            <PipelineNav
                                onPipelineSelect={handlePipelineSelect}
                                onNewChat={handleNewChat}
                                selectedPipelineId={selectedPipeline}
                                refreshChatsTrigger={refreshChatsTrigger}
                            />
                        </aside>
                    </ResizablePanel>
                    <ResizableHandle className="hidden lg:flex" withHandle />
                    <ResizablePanel defaultSize="90%" className="min-w-0 flex flex-col h-full min-h-0">
                        <div className="flex-shrink-0">
                            <AppHeader />
                        </div>

                        <main className="flex-1 min-h-0 flex flex-col overflow-hidden">
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
                                                                        className={`max-w-[85%] rounded-lg px-4 py-2 text-sm leading-6 ${message.role === "user"
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

                                            {/* Live stream steps + code – same resizable UI as in messages */}
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

                                            {/* Scroll anchor */}
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
                        </main>
                    </ResizablePanel>
                </ResizablePanelGroup>
            </div>

            {/* Authentication Dialog */}
            < AuthDialog open={showAuthDialog} onOpenChange={setShowAuthDialog} />
        </div >
    );
}
