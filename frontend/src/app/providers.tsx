"use client";

import { ThemeProvider } from "next-themes";
import { PipelineProvider } from "@/contexts/pipeline-context";
import { ReactNode } from "react";

export function Providers({ children }: { children: ReactNode }) {
  return (
    <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
      <PipelineProvider>{children}</PipelineProvider>
    </ThemeProvider>
  );
}
