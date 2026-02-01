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
      <p className="text-sm font-medium text-zinc-600 mb-4 text-center">
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
            className="group w-full rounded-xl border border-zinc-200 bg-gradient-to-br from-white to-zinc-50 px-5 py-4 text-left shadow-sm hover:shadow-md hover:border-zinc-300 transition-all duration-200"
          >
            <div className="flex items-start gap-3">
              <div className="mt-0.5 flex-shrink-0 text-lg">{example.icon}</div>
              <div className="flex-1">
                <div className="text-sm font-medium text-zinc-900 group-hover:text-black">
                  {example.title}
                </div>
                <div className="mt-1 text-xs text-zinc-500 leading-relaxed">
                  {example.description}
                </div>
              </div>
              <div className="flex-shrink-0 text-zinc-400 group-hover:text-zinc-600 transition-colors">
                →
              </div>
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
