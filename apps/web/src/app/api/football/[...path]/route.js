const allowed = /^(health|features|clubs|matches|standings|model\/metrics(?:\/v2)?|prediction\/config|predict(?:\/v2)?|knn\/predictions|match-results|pipeline|airflow\/history|outlook|phases|players|players\/similar|valuation|reports|reports\/[^/]+\/[^/]+|data\/catalog|data\/rows)$/;
async function proxy(request, context) {
  const { path } = await context.params;
  const endpoint = path.join("/");
  if (!allowed.test(endpoint) || (request.method === "POST" && !["predict", "predict/v2", "valuation"].includes(endpoint)))
    return Response.json({ detail: "Không tìm thấy chức năng" }, { status: 404 });
  try {
    const response = await fetch(`${process.env.FOOTBALL_API_URL || "http://127.0.0.1:8000"}/${endpoint}${new URL(request.url).search}`, {
      method: request.method, cache: "no-store", signal: AbortSignal.timeout(60000),
      headers: { "Content-Type": "application/json" },
      ...(request.method === "POST" ? { body: await request.text() } : {}),
    });
    return new Response(await response.text(), { status: response.status, headers: { "Content-Type": "application/json" } });
  } catch {
    return Response.json({ detail: "Dịch vụ phân tích chưa sẵn sàng. Hãy thử lại sau." }, { status: 503 });
  }
}
export const GET = proxy;
export const POST = proxy;
