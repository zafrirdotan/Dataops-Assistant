"use client";

import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import { Loader2 } from "lucide-react";
import { PipelineCode } from "@/components/pipeline-code";
import { getPipeline, getPipelineCode } from "@/lib/pipeline-api";
import type { PipelineDetail } from "@/lib/pipeline-api";
import { formatScheduleToReadable } from "@/lib/schedule-format";

export default function PipelinePage() {
  const params = useParams();
  const pipelineId =
    typeof params?.pipelineId === "string" ? params.pipelineId : null;
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
        setCodeFiles(
          typeof code === "object" && code !== null
            ? (code as Record<string, string>)
            : {}
        );
        if (!p) setError("Pipeline not found");
      })
      .catch((e) => {
        if (!cancelled) {
          setError(
            e instanceof Error ? e.message : "Failed to load pipeline"
          );
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
    <div className="flex-1 min-h-0 flex flex-col overflow-auto p-6">
      {loading ? (
        <div className="flex flex-1 flex-col items-center justify-center gap-3">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          <p className="text-sm text-muted-foreground">Loading pipeline...</p>
        </div>
      ) : error ? (
        <div className="flex flex-1 flex-col items-center justify-center gap-3">
          <p className="text-sm text-muted-foreground">{error}</p>
        </div>
      ) : (
        <div className="flex flex-col gap-6">
          <header className="rounded-xl border border-border bg-gradient-to-br from-muted to-background px-6 py-5 shadow-sm">
            <h1 className="text-2xl font-semibold tracking-tight text-foreground">
              {pipeline?.name ?? pipelineId}
            </h1>
            {pipeline?.description && (
              <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                {pipeline.description}
              </p>
            )}
            <div className="mt-3 flex items-center gap-2">
              <span className="inline-flex items-center rounded-md bg-muted px-2.5 py-1 text-xs font-medium text-muted-foreground">
                {scheduleDisplay}
              </span>
            </div>
          </header>
          <div>
            <h2 className="text-sm font-medium text-foreground mb-2">Code</h2>
            {codeForDisplay ? (
              <PipelineCode code={codeForDisplay} />
            ) : (
              <p className="text-sm text-muted-foreground py-4">
                No code available for this pipeline.
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
