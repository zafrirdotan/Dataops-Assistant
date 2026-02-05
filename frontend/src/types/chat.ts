type ChatMessage = {
  role: "user" | "assistant" | "system" | "steps" | "code";
  content: string;
};

type StepEvent = {
  step_name: string;
  step_number: number;
  message: string;
  status: string;
  error?: string;
};

type SSEEvent = {
  event: string;
  data: Record<string, unknown>;
};
