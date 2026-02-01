"use client";

import { useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";

type PipelineCodeProps = {
  code?: string | null;
};

export function PipelineCode({ code }: PipelineCodeProps) {
  if (!code) return null;

  const files = useMemo(() => {
    if (code.trim().startsWith("{") && code.trim().endsWith("}")) {
      try {
        const parsed = JSON.parse(code) as Record<string, unknown>;
        return Object.entries(parsed).map(([key, value]) => ({
          name: key,
          content:
            typeof value === "string" ? value : JSON.stringify(value, null, 2),
        }));
      } catch {
        return [{ name: "pipeline.py", content: code }];
      }
    }
    return [{ name: "pipeline.py", content: code }];
  }, [code]);

  const [activeIndex, setActiveIndex] = useState(0);
  const activeFile = files[Math.min(activeIndex, files.length - 1)];
  const hasFence = activeFile.content.includes("```");
  const markdown = hasFence
    ? activeFile.content
    : `\n\n\
\`\`\`python\n${activeFile.content}\n\`\`\`\n`;

  return (
    <div>
      <div className="flex min-h-[240px] overflow-hidden rounded-md border border-black/10 bg-white">
        <div className="w-40 border-r border-black/10 bg-white">
          <div className="px-3 py-2 text-[11px] font-semibold text-zinc-500">
            Pipeline code
          </div>
          <div className="px-3 py-1 text-[11px] uppercase tracking-wide text-zinc-500">
            Files
          </div>
          <ul className="flex flex-col">
            {files.map((file, index) => {
              const isActive = index === activeIndex;
              return (
                <li key={file.name}>
                  <button
                    type="button"
                    onClick={() => setActiveIndex(index)}
                    className={`w-full px-3 py-2 text-left text-xs transition-colors ${
                      isActive
                        ? "bg-black text-white"
                        : "text-zinc-600 hover:bg-zinc-100"
                    }`}
                  >
                    {file.name}
                  </button>
                </li>
              );
            })}
          </ul>
        </div>
        <div className="flex-1 bg-white p-3 text-xs text-zinc-800">
          <div className="mb-2 text-xs font-semibold text-zinc-500">
            {activeFile.name}
          </div>
          <div className="max-h-[41rem] overflow-auto ">
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              rehypePlugins={[rehypeHighlight]}
              components={{
                pre: ({ children }) => (
                  <pre className="overflow-auto rounded-md bg-zinc-900 p-3 text-zinc-100">
                    {children}
                  </pre>
                ),
                code: ({ className, children }) => (
                  <code className={className ?? ""}>{children}</code>
                ),
              }}
            >
              {markdown}
            </ReactMarkdown>
          </div>
        </div>
      </div>
    </div>
  );
}
