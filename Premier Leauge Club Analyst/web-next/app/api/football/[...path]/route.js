const allowed = /^(health|features|clubs|matches|model\/metrics|predict|knn\/predictions|knn\/neighbors\/[A-Z]{3}|dashboard|pipeline|outlook|phases|transfers|transfers\/simulate|rfm\/(gmm|rules)|players|players\/similar|valuation|reports|reports\/[^/]+\/[^/]+|data\/catalog|data\/rows|streaming\/transfers)$/;
async function proxy(request, context) {
  const { path } = await context.params;
  const endpoint = path.join("/");
  if (!allowed.test(endpoint) || (request.method === "POST" && !["predict", "valuation", "transfers/simulate"].includes(endpoint)))
    return Response.json({ detail: "Không tìm thấy chức năng" }, { status: 404 });
  try {
    const response = await fetch(`${process.env.FOOTBALL_API_URL || "http://127.0.0.1:8000"}/${endpoint}${new URL(request.url).search}`, {
      method: request.method, cache: "no-store", signal: AbortSignal.timeout(20000),
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
