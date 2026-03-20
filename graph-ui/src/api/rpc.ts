/* JSON-RPC client — speaks the same protocol as MCP clients via POST /rpc */

let _nextId = 1;

export class RpcError extends Error {
  constructor(
    public code: number,
    message: string,
  ) {
    super(message);
    this.name = "RpcError";
  }
}

export async function callTool<T = unknown>(
  name: string,
  args: Record<string, unknown> = {},
): Promise<T> {
  const res = await fetch("/rpc", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: _nextId++,
      method: "tools/call",
      params: { name, arguments: args },
    }),
  });

  if (!res.ok) {
    throw new RpcError(-1, `HTTP ${res.status}: ${res.statusText}`);
  }

  const json = await res.json();

  if (json.error) {
    throw new RpcError(json.error.code ?? -1, json.error.message ?? "unknown");
  }

  /* MCP tool results are wrapped: { result: { content: [{ text: "..." }], isError?: true } } */
  const result = json?.result;
  if (result?.isError) {
    throw new RpcError(-1, result.content?.[0]?.text ?? "tool error");
  }

  const text = result?.content?.[0]?.text;
  if (text === undefined) {
    return result as T;
  }

  try {
    return JSON.parse(text) as T;
  } catch {
    // Tool returned plain text, not JSON — return as-is
    return text as T;
  }
}
