"use client";

import {
    createContext,
    useContext,
    useState,
    useCallback,
    useEffect,
    useRef,
    type ReactNode,
} from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { getChatByPipeline, getChatHistory, streamChat } from "@/lib/chat-api";
import { usePipelineContext } from "@/contexts/pipeline-context";
import type { ChatMessage, SSEEvent, StepEvent } from "@/types/chat";

export type SendMessageResult = {
    authRequired?: boolean;
    pipelineCreated?: string;
};

type ChatContextType = {
    messages: ChatMessage[];
    loadingHistory: boolean;
    chatId: string | null;
    selectedPipelineId: string | null;
    setChatId: (id: string | null) => void;
    setSelectedPipelineId: (id: string | null) => void;
    setSkipFetchForPipeline: (pipelineId: string) => void;
    appendMessage: (msg: ChatMessage) => void;
    newChat: () => void;
    steps: StepEvent[];
    pipelineCode: string | null;
    streamingMessage: string;
    pipelineError: string | null;
    isLoading: boolean;
    sendMessage: (message: string) => Promise<SendMessageResult>;
    finalizeStream: () => ChatMessage | null;
    resetStream: () => void;
};

const ChatContext = createContext<ChatContextType | undefined>(undefined);

type ChatProviderProps = {
    children: ReactNode;
    /** Called when chat_created or chat_name_updated is received so the chat list can refetch */
    onChatListInvalidate?: () => void;
};

export function ChatProvider({ children, onChatListInvalidate }: ChatProviderProps) {
    const router = useRouter();
    const searchParams = useSearchParams();
    const pipelineFromUrl = searchParams.get("pipeline");
    const { refreshPipelines } = usePipelineContext();

    const newChatRequestedRef = useRef(false);
    const skipFetchForPipelineIdRef = useRef<string | null>(null);
    const streamStateRef = useRef({
        steps: [] as StepEvent[],
        pipelineCode: null as string | null,
        streamingMessage: "",
    });

    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [loadingHistory, setLoadingHistory] = useState(false);
    const [chatId, setChatIdState] = useState<string | null>(null);
    const [steps, setSteps] = useState<StepEvent[]>([]);
    const [pipelineCode, setPipelineCode] = useState<string | null>(null);
    const [streamingMessage, setStreamingMessage] = useState("");
    const [pipelineError, setPipelineError] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);

    const setChatId = useCallback((id: string | null) => {
        setChatIdState(id);
    }, []);

    const setSelectedPipelineId = useCallback(
        (id: string | null) => {
            if (id) {
                router.replace(`/p/${encodeURIComponent(id)}`);
            } else {
                router.replace("/");
            }
        },
        [router],
    );

    const setSkipFetchForPipeline = useCallback((pipelineId: string) => {
        skipFetchForPipelineIdRef.current = pipelineId;
    }, []);

    const appendMessage = useCallback((msg: ChatMessage) => {
        setMessages((prev) => [...prev, msg]);
    }, []);

    const resetStream = useCallback(() => {
        setSteps([]);
        setPipelineCode(null);
        setStreamingMessage("");
        setPipelineError(null);
    }, []);

    const finalizeStream = useCallback((): ChatMessage | null => {
        return {
            role: "assistant",
            content: streamingMessage.trim(),
            extra_data: {
                steps,
                pipeline_code: pipelineCode ?? undefined,
            },
        };
    }, [streamingMessage, steps, pipelineCode]);

    const loadHistoryForPipeline = useCallback(
        async (pipelineId: string | null) => {
            if (!pipelineId) {
                newChatRequestedRef.current = false;
                setMessages([]);
                setChatIdState(null);
                setLoadingHistory(false);
                return;
            }
            if (newChatRequestedRef.current) return;
            if (skipFetchForPipelineIdRef.current === pipelineId) {
                skipFetchForPipelineIdRef.current = null;
                return;
            }
            try {
                setLoadingHistory(true);
                setMessages([]);
                setChatIdState(null);
                const { chat_id } = await getChatByPipeline(pipelineId);
                setChatIdState(chat_id);
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
        },
        [],
    );

    useEffect(() => {
        loadHistoryForPipeline(pipelineFromUrl);
    }, [pipelineFromUrl, loadHistoryForPipeline]);

    const sendMessage = useCallback(
        async (message: string): Promise<SendMessageResult> => {
            appendMessage({ role: "user", content: message });
            setSteps([]);
            setPipelineCode(null);
            setStreamingMessage("");
            setPipelineError(null);
            setIsLoading(true);
            streamStateRef.current = {
                steps: [],
                pipelineCode: null,
                streamingMessage: "",
            };

            let currentChatId = chatId;
            const setChatIdFromStream = (id: string) => {
                currentChatId = id;
                setChatIdState(id);
            };

            try {
                const result = await streamChat({
                    message,
                    chatId: currentChatId,
                    onEvent: (evt: SSEEvent) => {
                        if (evt.event === "chat_created") {
                            const data = evt.data as { chat_id?: string; name?: string };
                            if (data.chat_id) setChatIdFromStream(data.chat_id);
                            onChatListInvalidate?.();
                            return;
                        }
                        if (evt.event === "chat_name_updated") {
                            onChatListInvalidate?.();
                            return;
                        }
                        if (evt.event === "step") {
                            const stepData = evt.data as StepEvent;
                            setSteps((prev) => {
                                const idx = prev.findIndex(
                                    (s) => s.step_name === stepData.step_name,
                                );
                                const next =
                                    idx === -1
                                        ? [...prev, stepData]
                                        : (() => {
                                            const n = [...prev];
                                            n[idx] = { ...n[idx], ...stepData };
                                            return n;
                                        })();
                                streamStateRef.current.steps = next;
                                return next;
                            });
                            return;
                        }
                        if (evt.event === "llm") {
                            const delta = String(
                                (evt.data as { delta?: string }).delta ?? "",
                            );
                            if (delta) {
                                setStreamingMessage((prev) => {
                                    const next = prev + delta;
                                    streamStateRef.current.streamingMessage = next;
                                    return next;
                                });
                            }
                            return;
                        }
                        if (evt.event === "final") {
                            const data = evt.data as {
                                pipeline_id?: string;
                                pipeline_code?: unknown;
                                error?: string;
                            };
                            if (
                                data.pipeline_code === undefined ||
                                data.pipeline_code === null
                            ) {
                                setPipelineCode(null);
                                streamStateRef.current.pipelineCode = null;
                            } else if (typeof data.pipeline_code === "string") {
                                setPipelineCode(data.pipeline_code);
                                streamStateRef.current.pipelineCode = data.pipeline_code;
                            } else {
                                const codeStr = JSON.stringify(data.pipeline_code, null, 2);
                                setPipelineCode(codeStr);
                                streamStateRef.current.pipelineCode = codeStr;
                            }
                            if (data.pipeline_id) {
                                skipFetchForPipelineIdRef.current = data.pipeline_id;
                                setSelectedPipelineId(data.pipeline_id);
                                refreshPipelines();
                            }
                            if (data.error) {
                                if (data.error.includes("Pipeline tests failed")) {
                                    setPipelineError(data.error);
                                } else {
                                    appendMessage({ role: "system", content: data.error ?? "" });
                                }
                            }
                        }
                    },
                });

                if (result.authRequired) {
                    setIsLoading(false);
                    return { authRequired: true };
                }

                if (result.pipelineId) {
                    setIsLoading(false);
                    return { pipelineCreated: result.pipelineId };
                }

                const {
                    steps: finalSteps,
                    pipelineCode: finalCode,
                    streamingMessage: finalContent,
                } = streamStateRef.current;
                const finalized: ChatMessage = {
                    role: "assistant",
                    content: finalContent.trim(),
                    extra_data: {
                        steps: finalSteps,
                        pipeline_code: finalCode ?? undefined,
                    },
                };
                appendMessage(finalized);
                resetStream();
                setIsLoading(false);
                return {};
            } catch (error) {
                appendMessage({
                    role: "system",
                    content: error instanceof Error ? error.message : "Stream error",
                });
                resetStream();
                setIsLoading(false);
                return {};
            }
        },
        [
            chatId,
            appendMessage,
            setSelectedPipelineId,
            refreshPipelines,
            resetStream,
        ],
    );

    const newChat = useCallback(() => {
        newChatRequestedRef.current = true;
        setMessages([]);
        setChatIdState(null);
        resetStream();
        setSelectedPipelineId(null);
    }, [setSelectedPipelineId, resetStream]);

    const value: ChatContextType = {
        messages,
        loadingHistory,
        chatId,
        selectedPipelineId: pipelineFromUrl,
        setChatId,
        setSelectedPipelineId,
        setSkipFetchForPipeline,
        appendMessage,
        newChat,
        steps,
        pipelineCode,
        streamingMessage,
        pipelineError,
        isLoading,
        sendMessage,
        finalizeStream,
        resetStream,
    };

    return <ChatContext.Provider value={value}>{children}</ChatContext.Provider>;
}

export function useChat(): ChatContextType {
    const context = useContext(ChatContext);
    if (context === undefined) {
        throw new Error("useChat must be used within a ChatProvider");
    }
    return context;
}
