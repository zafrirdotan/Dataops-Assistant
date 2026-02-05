export const runtime = "nodejs";

const BACKEND_URL = process.env.DATAOPS_BACKEND_URL ?? "http://localhost:8080";

export async function GET(
  req: Request,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const { path } = await params;
  const url = `${BACKEND_URL}/api/auth/${path.join("/")}`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      cookie: req.headers.get("cookie") || "",
    },
  });

  const data = await response.text();
  const headers = new Headers();

  // Forward Set-Cookie headers
  response.headers.forEach((value, key) => {
    if (key.toLowerCase() === "set-cookie") {
      headers.append("set-cookie", value);
    }
  });

  headers.set(
    "content-type",
    response.headers.get("content-type") || "application/json",
  );

  return new Response(data, {
    status: response.status,
    headers,
  });
}

export async function POST(
  req: Request,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const { path } = await params;
  const url = `${BACKEND_URL}/api/auth/${path.join("/")}`;
  const body = await req.text();

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": req.headers.get("content-type") || "application/json",
      cookie: req.headers.get("cookie") || "",
    },
    body: body || undefined,
  });

  const data = await response.text();
  const headers = new Headers();

  // Forward Set-Cookie headers
  response.headers.forEach((value, key) => {
    if (key.toLowerCase() === "set-cookie") {
      headers.append("set-cookie", value);
    }
  });

  headers.set(
    "content-type",
    response.headers.get("content-type") || "application/json",
  );

  return new Response(data, {
    status: response.status,
    headers,
  });
}
