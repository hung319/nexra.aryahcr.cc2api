import { serve } from "bun";

// --- CONFIGURATION ---
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.PROXY_API_KEY; 
const NEXRA_URL = process.env.NEXRA_URL || "https://nexra.aryahcr.cc/api/chat/completions";

// Chỉ định nghĩa đúng 4 model bạn yêu cầu
const ALLOWED_MODELS = [
  { id: "chatgpt", name: "Nexra ChatGPT" },
  { id: "gemini",  name: "Nexra Gemini" },
  { id: "qwen",    name: "Nexra Qwen" },
  { id: "chat",    name: "Nexra Chat" }
];

// --- HELPER FUNCTIONS ---
const createStreamFrame = (id: string, model: string, content: string | null, finish_reason: string | null) => {
  return JSON.stringify({
    id,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, delta: content ? { content } : {}, finish_reason }],
  });
};

const unauthorizedResponse = () => 
  new Response(JSON.stringify({ error: { message: "Invalid API Key", type: "invalid_request_error" } }), { status: 401, headers: { "Content-Type": "application/json" } });

// --- MAIN SERVER ---
console.log(`🚀 Proxy starting on port ${PORT}`);
console.log(`📋 Supported Models: ${ALLOWED_MODELS.map(m => m.id).join(", ")}`);

serve({
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);

    // 1. CORS Preflight
    if (req.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
      });
    }

    // 2. Auth Middleware
    if (API_KEY) {
      const authHeader = req.headers.get("Authorization");
      if (!authHeader || (!authHeader.includes(API_KEY) && authHeader !== API_KEY)) {
        return unauthorizedResponse();
      }
    }

    // 3. Endpoint: GET /v1/models
    // Chỉ trả về list 4 model đã định nghĩa
    if (req.method === "GET" && url.pathname === "/v1/models") {
      return new Response(JSON.stringify({
        object: "list",
        data: ALLOWED_MODELS.map(m => ({
          id: m.id, 
          object: "model",
          created: 1677610602,
          owned_by: "nexra-proxy",
          permission: [],
          root: m.id,
          parent: null,
        }))
      }), { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } });
    }

    // 4. Endpoint: POST /v1/chat/completions
    if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
      try {
        const body = await req.json();
        
        // Sử dụng trực tiếp model client gửi lên
        // (Client phải gửi đúng: "chatgpt", "gemini", "qwen" hoặc "chat")
        const targetModel = body.model; 
        const isStream = body.stream === true;

        console.log(`[Req] Model: "${targetModel}" | Stream: ${isStream}`);

        // Payload gửi sang Nexra
        const upstreamPayload = {
          messages: body.messages,
          model: targetModel,
          markdown: false,
          stream: isStream
        };

        const upstreamRes = await fetch(NEXRA_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(upstreamPayload),
        });

        if (!upstreamRes.ok) {
           const errText = await upstreamRes.text();
           console.error("[Upstream Error]", errText);
           return new Response(errText, { status: upstreamRes.status });
        }

        // --- Handle NON-STREAMING ---
        if (!isStream) {
            const data = await upstreamRes.json();
            const content = data.message || data.gpt || "";
            return new Response(JSON.stringify({
                id: `chatcmpl-${Date.now()}`,
                object: "chat.completion",
                created: Math.floor(Date.now() / 1000),
                model: targetModel,
                choices: [{ index: 0, message: { role: "assistant", content }, finish_reason: "stop" }],
                usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }
            }), { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } });
        }

        // --- Handle STREAMING (SSE Transform) ---
        const reqId = `chatcmpl-${Math.random().toString(36).substring(2, 10)}`;
        const reader = upstreamRes.body?.getReader();
        const encoder = new TextEncoder();
        const decoder = new TextDecoder();

        if (!reader) return new Response("Stream init failed", { status: 500 });

        const stream = new ReadableStream({
          async start(controller) {
            let buffer = "";
            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split("\n");
                buffer = lines.pop() || ""; // Giữ lại phần chưa trọn vẹn

                for (const line of lines) {
                  const trimmed = line.trim();
                  if (!trimmed.startsWith("data:")) continue;
                  
                  const jsonStr = trimmed.replace("data:", "").trim();
                  if (jsonStr === "[DONE]") continue;

                  try {
                    const data = JSON.parse(jsonStr);
                    // Format của Nexra: { message: "...", finish: boolean }
                    if (data.message) {
                        const chunk = createStreamFrame(reqId, targetModel, data.message, null);
                        controller.enqueue(encoder.encode(`data: ${chunk}\n\n`));
                    }
                    if (data.finish) {
                        const endChunk = createStreamFrame(reqId, targetModel, null, "stop");
                        controller.enqueue(encoder.encode(`data: ${endChunk}\n\n`));
                        controller.enqueue(encoder.encode("data: [DONE]\n\n"));
                        return;
                    }
                  } catch (e) { /* ignore parse error */ }
                }
              }
            } catch (e) {
              console.error("Stream error", e);
              controller.enqueue(encoder.encode(`data: [DONE]\n\n`));
            } finally {
              controller.close();
            }
          },
        });

        return new Response(stream, {
          headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
          },
        });

      } catch (error) {
        console.error("[Server Error]", error);
        return new Response(JSON.stringify({ error: "Internal Server Error" }), { status: 500 });
      }
    }

    return new Response("Not Found", { status: 404 });
  },
});
