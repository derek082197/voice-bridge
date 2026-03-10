// =============================================================================
// Voice Bridge Server — Railway.app
// Telnyx requires MP3 audio sent back, max once per second
// OpenAI outputs g711_ulaw — we buffer and convert to MP3 via ffmpeg
// =============================================================================

const express = require("express");
const { WebSocketServer, WebSocket } = require("ws");
const http    = require("http");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
if (!OPENAI_API_KEY) { console.error("❌ OPENAI_API_KEY required"); process.exit(1); }

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

app.get("/", (req, res) => res.json({ status: "Voice Bridge Running ✅", connections: wss.clients.size }));

wss.on("connection", (telnyxWs) => {
  console.log("📲 Telnyx connected");

  let openaiWs      = null;
  let streamSid     = null;
  let pingInterval  = null;
  let openaiReady   = false;
  let greetingSent  = false;
  let firstName     = "";
  let state         = "";



  function sendGreeting() {
    if (greetingSent || !openaiReady || !streamSid) return;
    greetingSent = true;
    console.log("🗣️ Sending greeting");
    openaiWs.send(JSON.stringify({
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "The call just connected. Say your opening greeting now." }],
      },
    }));
    openaiWs.send(JSON.stringify({ type: "response.create" }));
  }

  function connectOpenAI() {
    console.log("🔌 Connecting to OpenAI Realtime...");

    openaiWs = new WebSocket(
      "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17",
      { headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "OpenAI-Beta": "realtime=v1" } }
    );

    openaiWs.on("open", () => {
      console.log("✅ OpenAI connected");
      openaiReady = true;

      openaiWs.send(JSON.stringify({
        type: "session.update",
        session: {
          turn_detection: { type: "server_vad", threshold: 0.5, prefix_padding_ms: 300, silence_duration_ms: 600 },
          input_audio_format: "g711_ulaw",
          output_audio_format: "g711_ulaw",
          input_audio_transcription: { model: "whisper-1" },
          voice: "shimmer",
          instructions: buildSystemPrompt(firstName, state),
          modalities: ["text", "audio"],
          temperature: 0.7,
        },
      }));

      sendGreeting();

      pingInterval = setInterval(() => {
        if (openaiWs?.readyState === WebSocket.OPEN) openaiWs.send(JSON.stringify({ type: "ping" }));
      }, 25_000);
    });

    openaiWs.on("message", (data) => {
      try {
        const msg = JSON.parse(data);
        switch (msg.type) {

          case "response.audio.delta":
            // Send audio immediately — no buffering, minimum latency
            if (streamSid && telnyxWs.readyState === WebSocket.OPEN) {
              // Split into smaller chunks for smoother playback
              const chunkSize = 160; // 20ms of audio at 8kHz
              const buf = Buffer.from(msg.delta, "base64");
              for (let i = 0; i < buf.length; i += chunkSize) {
                const chunk = buf.slice(i, i + chunkSize);
                telnyxWs.send(JSON.stringify({
                  event: "media",
                  stream_id: streamSid,
                  media: { payload: chunk.toString("base64") },
                }));
              }
            }
            break;

          case "response.audio.done":
            if (streamSid && telnyxWs.readyState === WebSocket.OPEN) {
              telnyxWs.send(JSON.stringify({ event: "clear", stream_id: streamSid }));
            }
            console.log("✅ Agent finished speaking");
            break;

          case "conversation.item.input_audio_transcription.completed":
            console.log("👤 Caller:", msg.transcript);
            break;

          case "response.audio_transcript.done":
            console.log("🤖 Agent:", msg.transcript);
            break;

          case "session.updated":
            console.log("✅ Session configured");
            // Trigger greeting immediately after session is ready
            sendGreeting();
            break;

          case "error":
            console.error("❌ OpenAI error:", JSON.stringify(msg.error));
            break;
        }
      } catch (err) { console.error("OpenAI parse error:", err); }
    });

    openaiWs.on("error", (err) => { console.error("❌ OpenAI error:", err.message); openaiReady = false; });
    openaiWs.on("close", (code) => { console.log("OpenAI WS closed", code); openaiReady = false; if (pingInterval) clearInterval(pingInterval); });
  }

  telnyxWs.on("message", (data) => {
    try {
      const msg = JSON.parse(data);
      switch (msg.event) {
        case "connected":
          console.log("✅ Telnyx stream connected");
          connectOpenAI();
          break;

        case "start":
          streamSid = msg.stream_id ?? msg.streamSid ?? msg.start?.streamSid;
          firstName = msg.start?.customParameters?.firstName ?? msg.start?.customParameters?.first_name ?? "";
          state     = msg.start?.customParameters?.state ?? "";
          console.log(`▶️ SID: ${streamSid} | "${firstName}" "${state}"`);
          sendGreeting();
          break;

        case "media":
          if (greetingSent && openaiReady && openaiWs?.readyState === WebSocket.OPEN) {
            openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
          }
          break;

        case "stop":
          console.log("⏹️ Stream stopped");
          cleanup();
          break;
      }
    } catch (err) { console.error("Telnyx parse error:", err); }
  });

  telnyxWs.on("close", () => { console.log("Telnyx WS closed"); cleanup(); });
  telnyxWs.on("error", (err) => console.error("❌ Telnyx error:", err.message));

  function cleanup() {
    if (pingInterval) clearInterval(pingInterval);
    if (openaiWs?.readyState === WebSocket.OPEN) openaiWs.close();
  }
});

function buildSystemPrompt(firstName, state) {
  const hasName = firstName && firstName.trim().length > 0;
  const greeting = hasName
    ? `Hello? ... Hi ${firstName}, I'm calling from Premier Health Enrollment Center, ${state}. This short call is about your newly updated Marketplace benefits for 2026, and I believe you already have insurance through the Marketplace, right?`
    : `Hello? ... Hi, I'm calling from Premier Health Enrollment Center, ${state || "your area"}. This short call is about your newly updated Marketplace benefits for 2026, and I believe you already have insurance through the Marketplace, right?`;

  return `You are a friendly, professional AI voice agent for Premier Health Enrollment Center. Speak naturally — short sentences only, never use lists or markdown.

STEP 1 — OPENING GREETING (say word for word):
"${greeting}"

STEP 2 — IF THEY CONFIRM (yes/right/correct/uh huh):
"I understand. Just to verify your eligibility, are you currently under the age of 65?"

STEP 3A — IF UNDER 65:
"Perfect. You qualify for the benefits. Let me transfer you to the benefits specialist from your area."
Then stop — transfer will happen.

STEP 3B — IF 65+ OR ON MEDICARE:
"Oh I understand, unfortunately this particular program is for folks without those coverages. I apologize for the inconvenience. Have a great day!"

STEP 4 — IF NOT INTERESTED:
"No problem at all! Have a great day."

RULES:
- Never go off script
- Never say you are an AI
- Max two sentences per response
- Warm, friendly, professional tone`;
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Voice Bridge running on port ${PORT}`));
