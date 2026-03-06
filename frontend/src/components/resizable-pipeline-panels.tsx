"use client";

import { useCallback, useMemo, useRef, useEffect, type ReactNode } from "react";
import {
    ResizablePanelGroup,
    ResizablePanel,
    ResizableHandle,
} from "@/components/ui/resizable";

const STORAGE_KEY = "pipeline-panels-layout";
const STEPS_PANEL_ID = "steps";
const CODE_PANEL_ID = "code";
const DEFAULT_STEPS_SIZE = 20;
const DEFAULT_CODE_SIZE = 80;

function getStoredLayout(): { steps: number; code: number } | null {
    if (typeof window === "undefined") return null;
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw) as { steps?: number; code?: number };
        if (
            typeof parsed?.steps === "number" &&
            typeof parsed?.code === "number" &&
            parsed.steps >= 20 &&
            parsed.steps <= 80 &&
            parsed.code >= 20 &&
            parsed.code <= 80
        ) {
            return { steps: parsed.steps, code: parsed.code };
        }
    } catch {
        // ignore
    }
    return null;
}

function saveLayout(layout: { steps: number; code: number }) {
    try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(layout));
    } catch {
        // ignore
    }
}

type ResizablePipelinePanelsProps = {
    /** Content for the left panel (e.g. pipeline steps). */
    left?: ReactNode;
    /** Content for the right panel (e.g. pipeline code). */
    right?: ReactNode;
    className?: string;
};

/**
 * Wraps left/right content in a resizable two-panel layout (IDE-style).
 * If only one side is provided, renders that content full-width with no splitter.
 */
export function ResizablePipelinePanels({
    left,
    right,
    className,
}: ResizablePipelinePanelsProps) {
    const hasLeft = left != null;
    const hasRight = right != null;
    const both = hasLeft && hasRight;

    const defaultLayout = useMemo(() => {
        const stored = getStoredLayout();
        if (stored) return { [STEPS_PANEL_ID]: stored.steps, [CODE_PANEL_ID]: stored.code };
        return { [STEPS_PANEL_ID]: DEFAULT_STEPS_SIZE, [CODE_PANEL_ID]: DEFAULT_CODE_SIZE };
    }, []);

    const onLayoutChanged = useCallback((layout: Record<string, number>) => {
        // #region agent log
        fetch('http://127.0.0.1:7242/ingest/42983fc4-d75b-4d3a-9f9e-931a4fb75978', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Debug-Session-Id': '61f961' }, body: JSON.stringify({ sessionId: '61f961', location: 'resizable-pipeline-panels.tsx:onLayoutChanged', message: 'onLayoutChanged fired', data: { layout, keys: Object.keys(layout) }, timestamp: Date.now(), hypothesisId: 'H4' }) }).catch(() => { });
        // #endregion
        const stepsPct = layout[STEPS_PANEL_ID];
        const codePct = layout[CODE_PANEL_ID];
        if (typeof stepsPct === "number" && typeof codePct === "number") {
            saveLayout({ steps: stepsPct, code: codePct });
        }
    }, []);

    const wrapperRef = useRef<HTMLDivElement>(null);
    useEffect(() => {
        if (!both || !wrapperRef.current) return;
        const el = wrapperRef.current;
        const group = el.querySelector("[data-group]") as HTMLElement | null;
        const sep = el.querySelector("[data-separator]") as HTMLElement | null;
        // #region agent log
        fetch('http://127.0.0.1:7242/ingest/42983fc4-d75b-4d3a-9f9e-931a4fb75978', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Debug-Session-Id': '61f961' }, body: JSON.stringify({ sessionId: '61f961', location: 'resizable-pipeline-panels.tsx:useEffect', message: 'Rendering both + Group/Separator dimensions', data: { defaultLayout, hasLeft, hasRight, wrapperRect: el.getBoundingClientRect(), wrapperOffset: { width: el.offsetWidth, height: el.offsetHeight }, groupRect: group?.getBoundingClientRect() ?? null, groupOffset: group ? { width: group.offsetWidth, height: group.offsetHeight } : null, separatorRect: sep?.getBoundingClientRect() ?? null, separatorOffset: sep ? { width: sep.offsetWidth, height: sep.offsetHeight } : null }, timestamp: Date.now(), hypothesisId: 'H1' }) }).catch(() => { });
        fetch('http://127.0.0.1:7242/ingest/42983fc4-d75b-4d3a-9f9e-931a4fb75978', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Debug-Session-Id': '61f961' }, body: JSON.stringify({ sessionId: '61f961', location: 'resizable-pipeline-panels.tsx:useEffect', message: 'Group/Separator dimensions', data: { wrapperRect: el.getBoundingClientRect(), wrapperOffset: { width: el.offsetWidth, height: el.offsetHeight }, groupRect: group?.getBoundingClientRect() ?? null, groupOffset: group ? { width: group.offsetWidth, height: group.offsetHeight } : null, separatorRect: sep?.getBoundingClientRect() ?? null, separatorOffset: sep ? { width: sep.offsetWidth, height: sep.offsetHeight } : null }, timestamp: Date.now(), hypothesisId: 'H2' }) }).catch(() => { });
        // #endregion
    }, [both, defaultLayout, hasLeft, hasRight]);

    if (!hasLeft && !hasRight) return null;

    if (left && !right) {
        return (
            <div className="w-[30rem]">
                {left}
            </div>
        );
    }

    const stepsContent = (
        <div className="flex h-full flex-col min-h-0 overflow-auto px-4 py-3 w-full">
            {left}
        </div>
    );
    const codeContent = (
        <div className="h-full w-full min-w-0 overflow-auto p-3">{right}</div>
    );

    return (
        <div ref={wrapperRef} className={className}>
            {/* Mobile: stacked, each panel full width */}
            <div className="flex w-full flex-col gap-0 rounded-lg border border-border overflow-hidden bg-card min-h-[280px] md:hidden">
                <div className="min-h-[200px] border-b border-border">{stepsContent}</div>
                <div className="min-h-[280px] flex-1">{codeContent}</div>
            </div>
            {/* Desktop: resizable 25% steps / 75% code by default */}
            <div className="hidden md:block w-full">
                <ResizablePanelGroup
                    orientation="horizontal"
                    onLayoutChanged={onLayoutChanged}
                    className="h-full rounded-lg border border-border overflow-hidden bg-card"
                >
                    <ResizablePanel
                        defaultSize={DEFAULT_STEPS_SIZE}

                        className="rounded-l-lg min-w-0"
                    >
                        {stepsContent}
                    </ResizablePanel>
                    <ResizableHandle withHandle />
                    <ResizablePanel
                        defaultSize={DEFAULT_CODE_SIZE}

                        className="rounded-r-lg min-w-0"
                    >
                        {codeContent}
                    </ResizablePanel>
                </ResizablePanelGroup>
            </div>
        </div>
    );
}
