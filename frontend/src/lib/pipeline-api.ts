const API_BASE =
  typeof process !== "undefined"
    ? process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080"
    : "http://localhost:8080";

export type PipelineDetail = {
  id: number;
  pipeline_id: string;
  name: string;
  created_by: string;
  description: string;
  created_at: string;
  updated_at: string | null;
  status: string;
  spec: {
    schedule?: string;
    [key: string]: unknown;
  };
  image_id: string | null;
};

export type PipelineCodeFiles = {
  pipeline?: string;
  test_code?: string;
  requirements?: string;
  [key: string]: string | undefined;
};

export async function getPipeline(
  pipelineId: string
): Promise<PipelineDetail | null> {
  const res = await fetch(`${API_BASE}/pipeline/${pipelineId}`, {
    credentials: "include",
  });
  if (!res.ok) return null;
  return res.json();
}

export async function getPipelineCode(
  pipelineId: string
): Promise<PipelineCodeFiles> {
  const res = await fetch(`${API_BASE}/pipeline/${pipelineId}/code`, {
    credentials: "include",
  });
  if (!res.ok) return {};
  return res.json();
}
