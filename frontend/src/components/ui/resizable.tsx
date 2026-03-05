"use client";

import * as React from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import { GripVertical } from "lucide-react";
import { cn } from "@/lib/utils";

function ResizablePanelGroup({
  className,
  ...props
}: React.ComponentProps<typeof Group>) {
  return (
    <Group
      className={cn("flex h-full w-full data-[panel-group-direction=vertical]:flex-col", className)}
      {...props}
    />
  );
}

function ResizablePanel({
  className,
  ...props
}: React.ComponentProps<typeof Panel>) {
  return <Panel className={cn(className)} {...props} />;
}

function ResizableHandle({
  className,
  withHandle,
  onPointerDown,
  ...props
}: React.ComponentProps<typeof Separator> & { withHandle?: boolean }) {
  return (
    <Separator
      className={cn(
        "relative flex w-px shrink-0 items-center justify-center bg-zinc-200 after:absolute after:inset-y-0 after:left-1/2 after:w-3 after:-translate-x-1/2 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-zinc-400 focus-visible:ring-offset-1 data-[panel-group-direction=vertical]:h-px data-[panel-group-direction=vertical]:w-full data-[panel-group-direction=vertical]:after:left-0 data-[panel-group-direction=vertical]:after:h-1 data-[panel-group-direction=vertical]:after:w-full data-[panel-group-direction=vertical]:after:-translate-y-1/2 data-[panel-group-direction=vertical]:after:translate-x-0 [&[data-panel-group-direction=vertical]>div]:rotate-0 cursor-col-resize",
        className
      )}
      onPointerDown={(e) => {
        // #region agent log
        fetch('http://127.0.0.1:7242/ingest/42983fc4-d75b-4d3a-9f9e-931a4fb75978',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'61f961'},body:JSON.stringify({sessionId:'61f961',location:'resizable.tsx:ResizableHandle',message:'Pointer down on separator',data:{clientX:e.clientX,clientY:e.clientY},timestamp:Date.now(),hypothesisId:'H3'})}).catch(()=>{});
        // #endregion
        onPointerDown?.(e);
      }}
      {...props}
    >
      {withHandle && (
        <div className="z-10 flex h-4 w-3 items-center justify-center rounded-sm border border-zinc-300 bg-zinc-100 pointer-events-none">
          <GripVertical className="h-4 w-3 text-zinc-500" />
        </div>
      )}
    </Separator>
  );
}

export { ResizablePanelGroup, ResizablePanel, ResizableHandle };
