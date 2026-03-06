interface PipelineExample {
  icon: string;
  title: string;
  description: string;
}

interface PipelineExamplesProps {
  onSelectExample: (description: string) => void;
  onFocus?: () => void;
}

const examples: PipelineExample[] = [
  {
    icon: "📊",
    title: "CSV to Parquet & SQLite Pipeline",
    description:
      "Ingest ./data/*.csv to Parquet (partition by date) and SQLite table orders_daily, run daily at 02:00.",
  },
  {
    icon: "🔄",
    title: "Postgres Merge Pipeline",
    description:
      "From Postgres table public.transactions, merge into Postgres dw.fact_transactions by txn_id.",
  },
];

export function PipelineExamples({
  onSelectExample,
  onFocus,
}: PipelineExamplesProps) {
  return (
    <div className="mt-8">
      <p className="text-sm font-medium text-muted-foreground mb-4 text-center">
        Try these examples
      </p>
      <div className="flex flex-col gap-3">
        {examples.map((example, index) => (
          <button
            key={index}
            onClick={() => {
              onSelectExample(example.description);
              onFocus?.();
            }}
            className="group w-full rounded-xl border border-border bg-card bg-gradient-to-br from-background to-muted px-5 py-4 text-left shadow-sm hover:shadow-md hover:border-border transition-all duration-200"
          >
            <div className="flex items-start gap-3">
              <div className="mt-0.5 flex-shrink-0 text-lg">{example.icon}</div>
              <div className="flex-1">
                <div className="text-sm font-medium text-foreground group-hover:text-foreground">
                  {example.title}
                </div>
                <div className="mt-1 text-xs text-muted-foreground leading-relaxed">
                  {example.description}
                </div>
              </div>
              <div className="flex-shrink-0 text-muted-foreground group-hover:text-foreground transition-colors">
                →
              </div>
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
