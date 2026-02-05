"use client";

import { useEffect, useState, useCallback } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Button } from "@/components/ui/button";
import { Plus, Calendar, Clock } from "lucide-react";
import { cn } from "@/lib/utils";

type Pipeline = {
  id: number;
  pipeline_id: string;
  name: string;
  description: string;
  created_at: string;
  updated_at: string;
  status: string;
  spec: {
    schedule?: string;
  };
};

type PipelinesSidebarProps = {
  onPipelineSelect: (pipelineId: string) => void;
  onNewChat: () => void;
  selectedPipelineId: string | null;
  refreshTrigger?: number;
  className?: string;
};

export function PipelinesSidebar({
  onPipelineSelect,
  onNewChat,
  selectedPipelineId,
  refreshTrigger,
  className,
}: PipelinesSidebarProps) {
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchPipelines = useCallback(async () => {
    try {
      const response = await fetch("http://localhost:8080/pipelines", {
        method: "GET",
        credentials: "include", // Include cookies for authentication
      });

      if (response.ok) {
        const data = await response.json();
        setPipelines(data);
      }
    } catch (error) {
      console.error("Failed to fetch pipelines:", error);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPipelines();
  }, [fetchPipelines, refreshTrigger]);

  const formatDate = (dateString: string) => {
    try {
      const date = new Date(dateString);
      return date.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      });
    } catch {
      return "";
    }
  };

  return (
    <div
      className={cn(
        "flex h-full w-96 flex-col border-r border-zinc-200 bg-white",
        className,
      )}
    >
      <div className="flex items-center justify-between border-b border-zinc-200 p-4">
        <h2 className="text-lg font-semibold">Pipelines</h2>
        <Button
          size="sm"
          onClick={onNewChat}
          className="bg-black hover:bg-zinc-800"
        >
          <Plus className="mr-1 h-4 w-4" />
          New
        </Button>
      </div>

      <ScrollArea className="flex-1">
        {loading ? (
          <div className="p-4 text-center text-sm text-zinc-500">
            Loading...
          </div>
        ) : pipelines.length === 0 ? (
          <div className="p-4 text-center text-sm text-zinc-500">
            No pipelines yet. Start a new chat to create one!
          </div>
        ) : (
          <div className="space-y-1 ">
            {pipelines.map((pipeline) => (
              <button
                key={pipeline.pipeline_id}
                onClick={() => onPipelineSelect(pipeline.pipeline_id)}
                className={cn(
                  "w-96  p-3 text-left transition-all hover:border-zinc-300 hover:bg-zinc-50",
                  selectedPipelineId === pipeline.pipeline_id
                    ? "border-black bg-zinc-100"
                    : "border-zinc-200 bg-white",
                )}
              >
                <div className="mb-2 flex items-start justify-between gap-2">
                  <h3 className="flex-1 truncate text-sm font-medium">
                    {pipeline.name}
                  </h3>
                  <span
                    className={cn(
                      "flex-shrink-0 rounded-full px-2 py-0.5 text-xs",
                      pipeline.status === "deployed"
                        ? "bg-green-100 text-green-700"
                        : pipeline.status === "draft"
                          ? "bg-zinc-100 text-zinc-700"
                          : "bg-yellow-100 text-yellow-700",
                    )}
                  >
                    {pipeline.status}
                  </span>
                </div>

                {pipeline.description && (
                  <p className="mb-2 line-clamp-2 text-xs text-zinc-600">
                    {pipeline.description}
                  </p>
                )}

                <div className="flex items-center gap-3 text-xs text-zinc-500">
                  <div className="flex items-center gap-1">
                    <Clock className="h-3 w-3" />
                    {formatDate(pipeline.created_at)}
                  </div>
                  {pipeline.spec?.schedule && (
                    <div className="flex items-center gap-1">
                      <Calendar className="h-3 w-3" />
                      {pipeline.spec.schedule}
                    </div>
                  )}
                </div>
              </button>
            ))}
          </div>
        )}
      </ScrollArea>
    </div>
  );
}
