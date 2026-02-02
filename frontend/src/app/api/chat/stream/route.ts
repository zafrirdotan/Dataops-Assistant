export const runtime = "nodejs";

export async function POST(req: Request) {
  const body = await req.json();
  const backendUrl = process.env.DATAOPS_BACKEND_URL ?? "http://localhost:8080";

  const response = await fetch(`${backendUrl}/chat/stream`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Cookie: req.headers.get("cookie") || "", // Forward cookies to backend
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text();
    return new Response(text, { status: response.status });
  }

  if (!response.body) {
    return new Response("No stream body", { status: 502 });
  }

  return new Response(response.body, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    },
  });
}
