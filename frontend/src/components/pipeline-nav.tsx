"use client";

import { ScrollArea } from "@/components/ui/scroll-area";
import { Button } from "@/components/ui/button";
import { usePipelineContext } from "@/contexts/pipeline-context";
import { useAuth } from "@/hooks/useAuth";
import { useRouter } from "next/navigation";
import { Badge } from "@/components/ui/badge";
import { Loader2, AlertCircle } from "lucide-react";
import { useState } from "react";

const examplePipelines = [
  {
    pipeline_id: "example-1",
    name: "Daily Orders Ingest",
    description: "Automated daily order processing and storage",
    status: "deployed",
    created_at: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
    spec: { schedule: "0 0 * * *" },
  },
  {
    pipeline_id: "example-2",
    name: "Customer Sync",
    description: "Sync customer data from multiple sources",
    status: "deployed",
    created_at: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(),
    spec: { schedule: "0 */6 * * *" },
  },
  {
    pipeline_id: "example-3",
    name: "Payments Snapshot",
    description: "Hourly payment transaction snapshots",
    status: "draft",
    created_at: new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString(),
    spec: { schedule: "0 * * * *" },
  },
];

type PipelineNavProps = {
  onPipelineSelect?: (pipelineId: string) => void;
  onNewChat?: () => void;
  selectedPipelineId?: string | null;
};

export function PipelineNav({
  onPipelineSelect,
  onNewChat,
  selectedPipelineId,
}: PipelineNavProps) {
  const { pipelines, loading, error, refreshPipelines } = usePipelineContext();
  const { user, loading: authLoading } = useAuth();
  const router = useRouter();
  const [showAuthPrompt, setShowAuthPrompt] = useState(false);

  const isAuthenticated = !!user;
  const displayPipelines = isAuthenticated ? pipelines : examplePipelines;

  const formatRelativeTime = (dateString: string) => {
    const date = new Date(dateString);
    const now = new Date();
    const diffInMs = now.getTime() - date.getTime();
    const diffInDays = Math.floor(diffInMs / (1000 * 60 * 60 * 24));

    if (diffInDays === 0) return "Today";
    if (diffInDays === 1) return "Yesterday";
    if (diffInDays < 7) return `${diffInDays} days ago`;
    if (diffInDays < 30) return `${Math.floor(diffInDays / 7)} weeks ago`;
    return `${Math.floor(diffInDays / 30)} months ago`;
  };

  const handlePipelineClick = (pipelineId: string, isExample: boolean) => {
    if (isExample) {
      // Show auth prompt for example pipelines
      setShowAuthPrompt(true);
      router.push("/login");
      return;
    }
    onPipelineSelect?.(pipelineId);
  };

  if (authLoading || loading) {
    return (
      <nav className="flex h-full flex-col bg-white">
        <div className="px-4 py-3">
          <div className="text-xs text-zinc-500">Search pipelines</div>
        </div>
        <div className="flex flex-1 items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-zinc-400" />
        </div>
      </nav>
    );
  }

  if (error && isAuthenticated) {
    return (
      <nav className="flex h-full flex-col bg-white">
        <div className="px-4 py-3">
          <div className="text-xs text-zinc-500">Search pipelines</div>
        </div>
        <div className="flex flex-1 flex-col items-center justify-center px-4">
          <AlertCircle className="h-8 w-8 text-red-500 mb-2" />
          <p className="text-sm text-zinc-600 text-center mb-3">{error}</p>
          <Button size="sm" onClick={refreshPipelines} variant="outline">
            Retry
          </Button>
        </div>
      </nav>
    );
  }

  return (
    <nav className="flex h-full flex-col bg-white border-r border-zinc-200">
      <div className="px-4 py-4 border-b border-zinc-200">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-zinc-900">Pipelines</h2>
          {isAuthenticated && (
            <Button
              size="sm"
              variant="outline"
              onClick={() => onNewChat?.()}
              className="h-7 px-2 text-xs"
            >
              New
            </Button>
          )}
        </div>
      </div>
      <ScrollArea className="flex-1">
        {isAuthenticated && pipelines.length === 0 ? (
          <div className="flex flex-col items-center justify-center px-4 py-12 text-center">
            <p className="text-sm font-medium text-zinc-700 mb-1">
              No pipelines yet
            </p>
            <p className="text-xs text-zinc-500">
              Create your first pipeline to get started
            </p>
          </div>
        ) : (
          <ul className="flex flex-col">
            {displayPipelines.map((pipeline) => {
              const isExample = !isAuthenticated;
              const isSelected = selectedPipelineId === pipeline.pipeline_id;
              const schedule = pipeline.spec?.schedule;

              return (
                <li
                  key={pipeline.pipeline_id}
                  className={`px-4 py-3 border-b border-zinc-100 hover:bg-zinc-50 cursor-pointer transition-colors ${
                    isSelected ? "bg-zinc-100 border-l-4 border-l-black" : ""
                  }`}
                  onClick={() =>
                    handlePipelineClick(pipeline.pipeline_id, isExample)
                  }
                >
                  <div className="flex items-start justify-between gap-2 mb-1">
                    <div className="text-sm font-medium text-zinc-900 truncate">
                      {pipeline.name}
                    </div>
                    {isExample && (
                      <Badge variant="secondary" className="text-xs shrink-0">
                        Example
                      </Badge>
                    )}
                  </div>
                  {pipeline.description && (
                    <div className="text-xs text-zinc-600 mb-2 line-clamp-2">
                      {pipeline.description}
                    </div>
                  )}
                  <div className="flex flex-col gap-1 text-xs text-zinc-500">
                    <div className="flex items-center justify-between">
                      <span className="capitalize">{pipeline.status}</span>
                      <span>{formatRelativeTime(pipeline.created_at)}</span>
                    </div>
                    {schedule && (
                      <div className="flex items-center gap-1 text-xs text-zinc-500">
                        <svg
                          className="h-3 w-3"
                          fill="none"
                          stroke="currentColor"
                          viewBox="0 0 24 24"
                        >
                          <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            strokeWidth={2}
                            d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
                          />
                        </svg>
                        <span className="truncate">{schedule}</span>
                      </div>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </ScrollArea>
      <div className="px-4 py-3 border-t border-zinc-200 bg-zinc-50">
        {!isAuthenticated ? (
          <div className="text-xs text-zinc-600 text-center">
            <Button
              size="sm"
              variant="link"
              className="text-xs p-0 h-auto font-medium"
              onClick={() => router.push("/login")}
            >
              Sign in
            </Button>{" "}
            or{" "}
            <Button
              size="sm"
              variant="link"
              className="text-xs p-0 h-auto font-medium"
              onClick={() => router.push("/signup")}
            >
              sign up
            </Button>{" "}
            to create your own pipelines
          </div>
        ) : (
          <div className="text-xs text-zinc-500 text-center">
            {pipelines.length} pipeline{pipelines.length !== 1 ? "s" : ""}
          </div>
        )}
      </div>
    </nav>
  );
}
