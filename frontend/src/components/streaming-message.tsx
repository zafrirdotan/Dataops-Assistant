"use client";

import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";
import { PipelineSteps } from "@/components/pipeline-steps";
import { PipelineCode } from "@/components/pipeline-code";
import { ResizablePipelinePanels } from "@/components/resizable-pipeline-panels";
import { Button } from "@/components/ui/button";
import type { StepEvent } from "@/types/chat";


type StreamingMessageProps = {
    content: string;
    steps: StepEvent[];
    pipelineCode: string | null;
    pipelineError: string | null;
    onRetry?: () => void;
    isExiting?: boolean;
};

export function StreamingMessage({
    content,
    steps,
    pipelineCode,
    pipelineError,
    onRetry,
    isExiting = false,
}: StreamingMessageProps) {
    const showSteps = steps.length > 0;
    const showContent = content.length > 0;

    return (
        <div
            className={`transition-opacity duration-[250ms] ease-out ${isExiting ? "opacity-0" : "opacity-100"
                }`}
        >
            <div className="flex flex-col gap-4">
                {showContent && (
                    <div className="flex justify-start">
                        <div className="max-w-[85%] rounded-lg bg-zinc-100 px-4 py-2 text-sm leading-6 text-black">
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
                                        <code className={className ?? ""}>{children}</code>
                                    ),
                                }}
                            >
                                {content}
                            </ReactMarkdown>
                        </div>
                    </div>
                )}

                {(showSteps || pipelineCode) && (
                    <div className="flex justify-start">
                        <div className="w-full">
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

                {pipelineError && (
                    <div className="flex justify-start items-center gap-3">
                        <div className="max-w-[85%] rounded-lg border border-black bg-white px-4 py-2 text-sm text-black">
                            {pipelineError}
                        </div>
                        {onRetry && (
                            <Button
                                type="button"
                                variant="outline"
                                size="sm"
                                onClick={onRetry}
                            >
                                Retry
                            </Button>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
