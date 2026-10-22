const API_BASE = (import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000").replace(/\/$/, "");

export function apiUrl(path) {
  return API_BASE + path;
}

export async function getJson(path, params = {}, signal) {
  const url = new URL(apiUrl(path));
  Object.entries(params).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((item) => url.searchParams.append(key, item));
    } else if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value);
    }
  });
  const response = await fetch(url, { signal });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(body.detail || "API trả về lỗi " + response.status);
  }
  return body;
}

export async function getHealth(signal) {
  const response = await fetch(apiUrl("/health"), { signal });
  if (!response.ok) throw new Error("API PostgreSQL chưa sẵn sàng");
  return response.json();
}
