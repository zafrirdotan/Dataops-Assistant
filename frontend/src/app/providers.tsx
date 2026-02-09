"use client";

import { PipelineProvider } from "@/contexts/pipeline-context";
import { ReactNode } from "react";

export function Providers({ children }: { children: ReactNode }) {
  return <PipelineProvider>{children}</PipelineProvider>;
}
