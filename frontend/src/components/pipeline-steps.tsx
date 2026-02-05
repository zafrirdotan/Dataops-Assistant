type PipelineStepsProps = {
  steps: StepEvent[];
};

const statusClass: Record<string, string> = {
  pending: "bg-yellow-100 text-yellow-800",
  running: "bg-blue-100 text-blue-800",
  completed: "bg-green-100 text-green-800",
  failed: "bg-red-100 text-red-800",
};

export function PipelineSteps({ steps }: PipelineStepsProps) {
  return (
    <div className="flex flex-col gap-3">
      {steps.length === 0 && (
        <p className="text-sm text-zinc-400">No steps yet.</p>
      )}
      {steps.map((step) => (
        <div
          key={`${step.step_number}-${step.step_name}`}
          className="flex flex-col gap-1"
        >
          <div className="flex items-center justify-between text-xs">
            <span className="font-medium">
              {step.step_number}. {step.message}
            </span>
            <span
              className={`rounded border border-black/20 px-2 py-0.5 ${
                statusClass[step.status] ?? "bg-white"
              }`}
            >
              {step.status}
            </span>
          </div>
          <p className="text-xs text-zinc-500">{step.message}</p>
          {step.error && <p className="text-xs text-black">{step.error}</p>}
        </div>
      ))}
    </div>
  );
}
