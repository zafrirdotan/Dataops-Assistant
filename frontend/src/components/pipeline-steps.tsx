import type { StepEvent } from "@/types/chat";

type PipelineStepsProps = {
  steps: StepEvent[];
};

const statusClass: Record<string, string> = {
  pending: "bg-yellow-500/20 text-yellow-700 dark:text-yellow-400",
  running: "bg-blue-500/20 text-blue-700 dark:text-blue-400",
  completed: "bg-green-500/20 text-green-700 dark:text-green-400",
  failed: "bg-red-500/20 text-red-700 dark:text-red-400",
};

export function PipelineSteps({ steps }: PipelineStepsProps) {
  return (
    <div className="flex flex-col gap-3">
      {steps.length === 0 && (
        <p className="text-sm text-muted-foreground">No steps yet.</p>
      )}
      {steps.map((step) => (
        <div
          key={`${step.step_number}-${step.step_name}`}
          className="flex flex-col gap-1"
        >
          <div className="flex items-center justify-between text-xs">
            <span className="font-medium text-foreground">
              {step.step_number}. {step.message}
            </span>
            <span
              className={`rounded border border-border px-2 py-0.5 ${
                statusClass[step.status] ?? "bg-muted text-muted-foreground"
              }`}
            >
              {step.status}
            </span>
          </div>
          <p className="text-xs text-muted-foreground">{step.message}</p>
          {step.error && <p className="text-xs text-destructive">{step.error}</p>}
        </div>
      ))}
    </div>
  );
}
