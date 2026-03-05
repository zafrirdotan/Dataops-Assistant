export type ChatMessage = {
  role: "user" | "assistant" | "system" | "steps" | "code";
  chat_id?: string;
  created_at?: string;
  content: string;
  extra_data?: {
    type?: string;
    pipeline_id?: string;
    pipeline_code?: string;
    steps?: StepEvent[];
  };
};

export type StepEvent = {
  step_name: string;
  step_number: number;
  message: string;
  status: string;
  error?: string;
};

export type SSEEvent = {
  event: string;
  data: Record<string, unknown>;
};
