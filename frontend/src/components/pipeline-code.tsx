import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";

type PipelineCodeProps = {
  code?: string | null;
};

export function PipelineCode({ code }: PipelineCodeProps) {
  if (!code) return null;

  const hasFence = code.includes("```");
  const markdown = hasFence
    ? code
    : `\n\n\
\`\`\`python\n${code}\n\`\`\`\n`;

  return (
    <div>
      <div className="text-xs font-semibold text-zinc-500">Pipeline code</div>
      <div className="mt-3 max-h-60 overflow-auto rounded-md border border-black/10 bg-white p-3 text-xs text-zinc-800">
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
  );
}
