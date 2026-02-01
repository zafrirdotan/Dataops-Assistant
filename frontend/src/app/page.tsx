"use client";

import { useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";

type ChatMessage = {
  role: "user" | "assistant" | "system";
  content: string;
};

type StepEvent = {
  step: string;
  step_number: number;
  message: string;
  status: string;
  error?: string;
};

type SSEEvent = {
  event: string;
  data: Record<string, unknown>;
};

export default function Home() {
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [steps, setSteps] = useState<StepEvent[]>([]);
  const [pipelineId, setPipelineId] = useState<string | null>(null);
  const [pipelineCode, setPipelineCode] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const statusClass = useMemo(
    () =>
      ({
        started: "bg-black text-white",
        completed: "bg-zinc-200 text-black",
        error: "bg-black text-white",
        skipped: "bg-zinc-100 text-zinc-700",
      }) as Record<string, string>,
    [],
  );

  const parseSSEChunk = (chunk: string): SSEEvent | null => {
    let eventName = "message";
    let data = "";
    const lines = chunk.split("\n").filter(Boolean);
    for (const line of lines) {
      if (line.startsWith("event:")) {
        eventName = line.slice(6).trim();
      }
      if (line.startsWith("data:")) {
        data += line.slice(5).trim();
      }
    }
    if (!data) return null;
    try {
      return { event: eventName, data: JSON.parse(data) };
    } catch {
      return null;
    }
  };

  const handleEvent = (evt: SSEEvent) => {
    if (evt.event === "step") {
      const stepData = evt.data as StepEvent;
      setSteps((prev) => {
        const idx = prev.findIndex((s) => s.step === stepData.step);
        if (idx === -1) return [...prev, stepData];
        const next = [...prev];
        next[idx] = { ...next[idx], ...stepData };
        return next;
      });
      return;
    }

    if (evt.event === "llm") {
      const delta = String((evt.data as { delta?: string }).delta ?? "");
      if (!delta) return;
      setMessages((prev) => {
        const last = prev[prev.length - 1];
        if (last?.role === "assistant") {
          const next = [...prev];
          next[next.length - 1] = {
            ...last,
            content: last.content + delta,
          };
          return next;
        }
        return [...prev, { role: "assistant", content: delta }];
      });
      return;
    }

    if (evt.event === "final") {
      const data = evt.data as {
        pipeline_id?: string;
        pipeline_code?: unknown;
        success?: boolean;
        error?: string;
      };
      setPipelineId(data.pipeline_id ?? null);
      if (data.pipeline_code === undefined || data.pipeline_code === null) {
        setPipelineCode(null);
      } else if (typeof data.pipeline_code === "string") {
        setPipelineCode(data.pipeline_code);
      } else {
        setPipelineCode(JSON.stringify(data.pipeline_code, null, 2));
      }
      if (data.error) {
        setMessages((prev) => [
          ...prev,
          { role: "system", content: data.error ?? "" },
        ]);
      }
    }
  };

  const handleSubmit = async () => {
    const trimmed = input.trim();
    if (!trimmed || isLoading) return;

    setMessages((prev) => [...prev, { role: "user", content: trimmed }]);
    setInput("");
    setSteps([]);
    setPipelineId(null);
    setPipelineCode(null);
    setIsLoading(true);

    try {
      const res = await fetch("/api/chat/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: trimmed, fast: false }),
      });

      if (!res.ok || !res.body) {
        const text = await res.text();
        setMessages((prev) => [
          ...prev,
          { role: "system", content: text || "Request failed" },
        ]);
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() ?? "";
        for (const part of parts) {
          const evt = parseSSEChunk(part);
          if (evt) handleEvent(evt);
        }
      }
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        {
          role: "system",
          content: error instanceof Error ? error.message : "Stream error",
        },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-white text-black">
      <main className="mx-auto flex w-full max-w-6xl flex-col gap-6 px-6 py-10">
        <header className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-semibold">DataOps Assistant</h1>
            <p className="text-sm text-zinc-500">
              Streaming ETL assistant with live pipeline steps.
            </p>
          </div>
        </header>

        <div className="grid gap-6 lg:grid-cols-3">
          <Card className="lg:col-span-2">
            <CardHeader>
              <CardTitle>Chat</CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              <ScrollArea className="h-[420px] rounded-md border border-zinc-200 p-4">
                <div className="flex flex-col gap-3">
                  {messages.length === 0 && (
                    <div className="text-sm text-zinc-400">
                      Ask for an ETL pipeline. Example: “Load CSV from
                      ./data/transactions_1.csv to SQLite orders_daily, daily at
                      2am”.
                    </div>
                  )}
                  {messages.map((msg, idx) => (
                    <div
                      key={`${msg.role}-${idx}`}
                      className={`flex ${
                        msg.role === "user" ? "justify-end" : "justify-start"
                      }`}
                    >
                      <div
                        className={`max-w-[80%] rounded-lg px-4 py-2 text-sm leading-6 ${
                          msg.role === "user"
                            ? "bg-black text-white"
                            : msg.role === "system"
                              ? "border border-black bg-white text-black"
                              : "bg-zinc-100 text-black"
                        }`}
                      >
                        {msg.content}
                      </div>
                    </div>
                  ))}
                </div>
              </ScrollArea>

              <div className="flex flex-col gap-3">
                <Textarea
                  placeholder="Describe the pipeline you want..."
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  rows={3}
                />
                <div className="flex items-center justify-between">
                  <span className="text-xs text-zinc-500">
                    {isLoading ? "Streaming..." : "Ready"}
                  </span>
                  <Button onClick={handleSubmit} disabled={isLoading}>
                    Send
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          <div className="flex flex-col gap-6">
            <Card>
              <CardHeader>
                <CardTitle>Pipeline Steps</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-3">
                  {steps.length === 0 && (
                    <p className="text-sm text-zinc-400">No steps yet.</p>
                  )}
                  {steps.map((step) => (
                    <div key={step.step} className="flex flex-col gap-1">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium">
                          {step.step_number}. {step.step}
                        </span>
                        <Badge className={statusClass[step.status] ?? ""}>
                          {step.status}
                        </Badge>
                      </div>
                      <p className="text-xs text-zinc-500">{step.message}</p>
                      {step.error && (
                        <p className="text-xs text-black">{step.error}</p>
                      )}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Pipeline Output</CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-3">
                <div className="text-sm">
                  <span className="text-zinc-500">Pipeline ID:</span>{" "}
                  <span className="font-medium">{pipelineId ?? "—"}</span>
                </div>
                <div className="rounded-md border border-zinc-200 bg-zinc-50 p-3 text-xs text-zinc-700 max-h-[220px] overflow-auto whitespace-pre-wrap">
                  {pipelineCode ?? "Pipeline code will appear here."}
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </main>
    </div>
  );
}
