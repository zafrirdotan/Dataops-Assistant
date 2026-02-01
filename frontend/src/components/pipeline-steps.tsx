type StepEvent = {
  step: string;
  step_number: number;
  message: string;
  status: string;
  error?: string;
};

type PipelineStepsProps = {
  steps: StepEvent[];
  statusClass: Record<string, string>;
};

export function PipelineSteps({ steps, statusClass }: PipelineStepsProps) {
  return (
    <div className="flex flex-col gap-3">
      {steps.length === 0 && (
        <p className="text-sm text-zinc-400">No steps yet.</p>
      )}
      {steps.map((step) => (
        <div key={step.step} className="flex flex-col gap-1">
          <div className="flex items-center justify-between text-xs">
            <span className="font-medium">
              {step.step_number}. {step.step}
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
