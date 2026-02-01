import { ScrollArea } from "@/components/ui/scroll-area";

const placeholderPipelines = [
  {
    name: "Daily Orders Ingest",
    window: "Last 30 days",
    timezone: "Local time",
  },
  {
    name: "Customer Sync",
    window: "Last 7 days",
    timezone: "UTC",
  },
  {
    name: "Payments Snapshot",
    window: "Last 24 hours",
    timezone: "Local time",
  },
  {
    name: "Support Tickets ETL",
    window: "Last 14 days",
    timezone: "UTC",
  },
  {
    name: "Inventory Refresh",
    window: "Last 48 hours",
    timezone: "Local time",
  },
];

export function PipelineNav() {
  return (
    <nav className="flex h-full flex-col bg-white">
      <div className="px-4 py-3">
        <div className="text-xs text-zinc-500">Search pipelines</div>
      </div>
      <ScrollArea className="flex-1">
        <ul className="flex flex-col">
          {placeholderPipelines.map((pipeline) => (
            <li key={pipeline.name} className="px-4 py-3">
              <div className="text-sm font-medium text-black">
                {pipeline.name}
              </div>
              <div className="mt-1 flex items-center justify-between text-xs text-zinc-500">
                <span>{pipeline.window}</span>
                <span>{pipeline.timezone}</span>
              </div>
            </li>
          ))}
        </ul>
      </ScrollArea>
      <div className="px-4 py-3 text-xs text-zinc-500">
        Placeholder for future pipeline navigation.
      </div>
    </nav>
  );
}
