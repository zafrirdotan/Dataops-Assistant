"use client";

import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import { Loader2 } from "lucide-react";
import {
    ResizablePanelGroup,
    ResizablePanel,
    ResizableHandle,
} from "@/components/ui/resizable";
import { SideNav } from "@/components/side-nav";
import { AppHeader } from "@/components/app-header";
import { PipelineCode } from "@/components/pipeline-code";
import { getPipeline, getPipelineCode } from "@/lib/pipeline-api";
import type { PipelineDetail } from "@/lib/pipeline-api";
import { formatScheduleToReadable } from "@/lib/schedule-format";

export default function PipelinePage() {
    const params = useParams();
    const pipelineId = typeof params?.pipelineId === "string" ? params.pipelineId : null;
    const [pipeline, setPipeline] = useState<PipelineDetail | null>(null);
    const [codeFiles, setCodeFiles] = useState<Record<string, string>>({});
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        if (!pipelineId) {
            setLoading(false);
            setError("Missing pipeline ID");
            return;
        }
        let cancelled = false;
        setLoading(true);
        setError(null);
        Promise.all([getPipeline(pipelineId), getPipelineCode(pipelineId)])
            .then(([p, code]) => {
                if (cancelled) return;
                setPipeline(p ?? null);
                setCodeFiles(typeof code === "object" && code !== null ? (code as Record<string, string>) : {});
                if (!p) setError("Pipeline not found");
            })
            .catch((e) => {
                if (!cancelled) {
                    setError(e instanceof Error ? e.message : "Failed to load pipeline");
                }
            })
            .finally(() => {
                if (!cancelled) setLoading(false);
            });
        return () => {
            cancelled = true;
        };
    }, [pipelineId]);

    const scheduleRaw =
        pipeline?.spec?.schedule != null && pipeline.spec.schedule !== ""
            ? String(pipeline.spec.schedule)
            : "manual";
    const scheduleDisplay = formatScheduleToReadable(scheduleRaw);
    const codeForDisplay =
        Object.keys(codeFiles).length > 0
            ? JSON.stringify(codeFiles, null, 2)
            : null;

    return (
        <div className="h-screen overflow-hidden bg-white text-black flex flex-col">
            <div className="flex flex-1 min-h-0">
                <ResizablePanelGroup orientation="horizontal" className="flex-1 min-h-0">
                    <ResizablePanel defaultSize="20%" className="hidden lg:block min-w-0">
                        <aside className="h-full w-full">
                            <SideNav />
                        </aside>
                    </ResizablePanel>
                    <ResizableHandle className="hidden lg:flex" withHandle />
                    <ResizablePanel
                        defaultSize="90%"
                        className="min-w-0 flex flex-col h-full min-h-0"
                    >

                        <main className="flex-1 min-h-0 flex flex-col overflow-auto">
                            <div className="flex-shrink-0">
                                <AppHeader />
                            </div>
                            <div className="flex-1 min-h-0 flex flex-col overflow-auto p-6">
                                {loading ? (
                                    <div className="flex flex-1 flex-col items-center justify-center gap-3">
                                        <Loader2 className="h-8 w-8 animate-spin text-zinc-400" />
                                        <p className="text-sm text-zinc-500">Loading pipeline...</p>
                                    </div>
                                ) : error ? (
                                    <div className="flex flex-1 flex-col items-center justify-center gap-3">
                                        <p className="text-sm text-zinc-600">{error}</p>
                                    </div>
                                ) : (
                                    <div className="flex flex-col gap-6">
                                        <header className="rounded-xl border border-zinc-200/80 bg-gradient-to-br from-zinc-50 to-white px-6 py-5 shadow-sm">
                                            <h1 className="text-2xl font-semibold tracking-tight text-zinc-900">
                                                {pipeline?.name ?? pipelineId}
                                            </h1>
                                            {pipeline?.description && (
                                                <p className="mt-2 text-sm leading-relaxed text-zinc-600">
                                                    {pipeline.description}
                                                </p>
                                            )}
                                            <div className="mt-3 flex items-center gap-2">
                                                <span className="inline-flex items-center rounded-md bg-zinc-100 px-2.5 py-1 text-xs font-medium text-zinc-600">
                                                    {scheduleDisplay}
                                                </span>
                                            </div>
                                        </header>
                                        <div>
                                            <h2 className="text-sm font-medium text-zinc-700 mb-2">
                                                Code
                                            </h2>
                                            {codeForDisplay ? (
                                                <PipelineCode code={codeForDisplay} />
                                            ) : (
                                                <p className="text-sm text-zinc-500 py-4">
                                                    No code available for this pipeline.
                                                </p>
                                            )}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </main>
                    </ResizablePanel>
                </ResizablePanelGroup>
            </div>
        </div>
    );
}
