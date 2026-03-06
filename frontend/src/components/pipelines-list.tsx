"use client";

import { useRouter, usePathname } from "next/navigation";
import { Loader2 } from "lucide-react";
import { usePipelineContext } from "@/contexts/pipeline-context";

export function PipelinesList() {
  const { pipelines, loading, error } = usePipelineContext();
  const router = useRouter();
  const pathname = usePathname();

  const currentPipelineId =
    pathname?.startsWith("/p/") && pathname !== "/p"
      ? pathname.replace(/^\/p\/?/, "").split("/")[0]
      : null;

  const handlePipelineClick = (pipelineId: string) => {
    router.push(`/p/${pipelineId}`);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="px-4 py-4 text-xs text-muted-foreground">
        Failed to load pipelines.
      </div>
    );
  }

  if (pipelines.length === 0) {
    return (
      <div className="px-4 py-4 text-xs text-muted-foreground">
        No pipelines yet.
      </div>
    );
  }

  return (
    <ul className="flex flex-col">
      {pipelines.map((pipeline) => {
        const isSelected = currentPipelineId === pipeline.pipeline_id;
        const title = pipeline.name?.trim() || pipeline.pipeline_id;
        return (
          <li
            key={pipeline.pipeline_id}
            className={`px-3 cursor-pointer transition-colors py-2 rounded-lg ${
              isSelected ? "text-foreground bg-accent" : "text-muted-foreground"
            }`}
            onClick={() => handlePipelineClick(pipeline.pipeline_id)}
          >
            <div className="text-xs font-medium truncate">{title}</div>
          </li>
        );
      })}
    </ul>
  );
}
