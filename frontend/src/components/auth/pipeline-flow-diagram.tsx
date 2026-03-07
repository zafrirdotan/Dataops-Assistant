"use client";

import {
    FileSpreadsheet,
    Database,
    Globe,
    Bot,
    FileOutput,
    ChevronRight,
    CalendarClock,
    Cpu,
    RefreshCw,
} from "lucide-react";
import { cn } from "@/lib/utils";

const iconSize = 20;

const sourceNodes = [
    { label: "CSV", Icon: FileSpreadsheet },
    { label: "PostgreSQL", Icon: Database },
    { label: "API", Icon: Globe },
] as const;

const destinationNodes = [
    { label: "CSV", Icon: FileSpreadsheet },
    { label: "Parquet", Icon: FileOutput },
    { label: "SQLite", Icon: Database },
    { label: "PostgreSQL", Icon: Database },
] as const;

const engineOutcomeNodes = [
    { label: "Creates pipeline", Icon: Cpu },
    { label: "Airflow schedule", Icon: CalendarClock },
] as const;

function FlowNode({
    label,
    Icon,
    className,
}: {
    label: string;
    Icon: React.ComponentType<{ size?: number; className?: string }>;
    className?: string;
}) {
    return (
        <div
            className={cn(
                "flex flex-col items-center justify-center gap-1.5 rounded-lg border bg-card px-3 py-2.5 shadow-sm transition-colors hover:bg-accent/50",
                className
            )}
            aria-hidden
        >
            <Icon size={iconSize} className="text-muted-foreground" />
            <span className="text-xs font-medium text-foreground">{label}</span>
        </div>
    );
}

function Connector() {
    return (
        <div
            className="flex shrink-0 items-center text-muted-foreground/60"
            aria-hidden
        >
            <ChevronRight size={24} strokeWidth={2} />
        </div>
    );
}

export function PipelineFlowDiagram({ className }: { className?: string }) {
    return (
        <div
            className={cn("w-full max-w-xl", className)}
            role="img"
            aria-label="Pipeline flow: CSV, PostgreSQL, and API sources feed into AI Pipeline Engine, which creates the pipeline and adds an Airflow schedule, and outputs to CSV, Parquet, SQLite, and PostgreSQL"
        >
            {/* Conveyor / assembly line band behind center */}
            <div className="relative flex flex-col items-stretch gap-4 rounded-xl border bg-muted/30 p-4 md:flex-row md:items-center md:justify-between md:gap-2">
                {/* Optional conveyor stripe effect */}
                <div
                    className="absolute inset-0 overflow-hidden rounded-xl opacity-[0.03]"
                    aria-hidden
                >
                    <div
                        className="h-full w-full"
                        style={{
                            backgroundImage: `repeating-linear-gradient(
                90deg,
                transparent,
                transparent 8px,
                currentColor 8px,
                currentColor 10px
              )`,
                        }}
                    />
                </div>

                {/* Sources */}
                <div className="relative flex flex-col items-center justify-center gap-2  md:justify-end">
                    {sourceNodes.map(({ label, Icon }) => (
                        <FlowNode key={label} label={label} Icon={Icon} />
                    ))}
                </div>

                <Connector />

                {/* Center: LLM / Robot */}
                <div className="relative flex flex-col items-center justify-center gap-3">
                    <div className="flex flex-col items-center gap-2 rounded-xl border-2  border-primary/20 bg-primary/5 px-4 py-3 mb-4 shadow-md">
                        <div className="rounded-full bg-primary/10 p-2">
                            <Bot size={28} className="text-primary" />
                        </div>
                        <span className="text-center text-sm font-semibold text-foreground">
                            AI Pipeline Engine
                        </span>
                    </div>
                    <div className="flex flex-col items-center justify-center gap-2">
                        <FlowNode
                            label={engineOutcomeNodes[0].label}
                            Icon={engineOutcomeNodes[0].Icon}
                        />
                        <div
                            className="flex shrink-0 items-center justify-center rounded-full bg-muted/80 p-1.5 text-muted-foreground"
                            title="Cycle"
                            aria-hidden
                        >
                            <RefreshCw size={18} strokeWidth={2} />
                        </div>
                        <FlowNode
                            label={engineOutcomeNodes[1].label}
                            Icon={engineOutcomeNodes[1].Icon}
                        />
                    </div>
                </div>

                <Connector />

                {/* Destinations */}
                <div className="relative flex flex-wrap items-center justify-center gap-2 md:justify-start">
                    {destinationNodes.map(({ label, Icon }) => (
                        <FlowNode key={label} label={label} Icon={Icon} />
                    ))}
                </div>
            </div>
        </div>
    );
}
