const express = require("express");
const { WebSocketServer, WebSocket } = require("ws");
const http = require("http");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
if (!OPENAI_API_KEY) { console.error("❌ OPENAI_API_KEY required"); process.exit(1); }

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

app.get("/", (req, res) => res.json({ status: "Voice Bridge Running ✅", connections: wss.clients.size }));

wss.on("connection", (telnyxWs) => {
  console.log("📲 Telnyx connected");

  let openaiWs        = null;
  let streamSid       = null;
  let pingInterval    = null;
  let openaiReady     = false;
  let greetingSent    = false;
  let firstName       = "";
  let state           = "";
  const audioBuffer   = [];
  const pendingAudio  = []; // OpenAI audio waiting for streamSid

  function sendGreeting() {
    if (greetingSent || !openaiReady || !streamSid) return;
    greetingSent = true;
    console.log("🗣️ Sending greeting trigger");
    openaiWs.send(JSON.stringify({
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "The call just connected. Say your opening greeting now exactly as instructed." }],
      },
    }));
    openaiWs.send(JSON.stringify({ type: "response.create" }));

    // Flush any pending audio that arrived before streamSid
    if (pendingAudio.length > 0) {
      console.log(`📦 Flushing ${pendingAudio.length} pending audio chunks to Telnyx`);
      for (const delta of pendingAudio) {
        telnyxWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: delta } }));
      }
      pendingAudio.length = 0;
    }
  }

  function connectOpenAI() {
    console.log("🔌 Connecting to OpenAI Realtime...");

    openaiWs = new WebSocket(
      "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01",
      { headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "OpenAI-Beta": "realtime=v1" } }
    );

    openaiWs.on("open", () => {
      console.log("✅ OpenAI Realtime connected");
      openaiReady = true;

      openaiWs.send(JSON.stringify({
        type: "session.update",
        session: {
          turn_detection: { type: "server_vad", threshold: 0.5, prefix_padding_ms: 300, silence_duration_ms: 500 },
          input_audio_format: "g711_ulaw",
          output_audio_format: "g711_ulaw",
          input_audio_transcription: { model: "whisper-1" },
          voice: "shimmer",
          instructions: buildSystemPrompt(firstName, state),
          modalities: ["text", "audio"],
          temperature: 0.7,
        },
      }));

      // Flush buffered caller audio
      if (audioBuffer.length > 0) {
        console.log(`📦 Flushing ${audioBuffer.length} buffered caller audio chunks`);
        for (const chunk of audioBuffer) {
          openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: chunk }));
        }
        audioBuffer.length = 0;
      }

      // Only send greeting if streamSid already arrived
      sendGreeting();

      pingInterval = setInterval(() => {
        if (openaiWs?.readyState === WebSocket.OPEN) openaiWs.send(JSON.stringify({ type: "ping" }));
      }, 30_000);
    });

    openaiWs.on("message", (data) => {
      try {
        const msg = JSON.parse(data);
        switch (msg.type) {
          case "response.audio.delta":
            if (telnyxWs.readyState === WebSocket.OPEN) {
              if (streamSid) {
                telnyxWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: msg.delta } }));
              } else {
                // Buffer agent audio until streamSid arrives
                pendingAudio.push(msg.delta);
              }
            }
            break;
          case "response.audio.done":
            if (streamSid && telnyxWs.readyState === WebSocket.OPEN) {
              telnyxWs.send(JSON.stringify({ event: "clear", streamSid }));
            }
            break;
          case "conversation.item.input_audio_transcription.completed":
            console.log("👤 Caller:", msg.transcript);
            break;
          case "response.audio_transcript.done":
            console.log("🤖 Agent:", msg.transcript);
            break;
          case "error":
            console.error("❌ OpenAI error:", JSON.stringify(msg.error));
            break;
        }
      } catch (err) { console.error("OpenAI parse error:", err); }
    });

    openaiWs.on("error", (err) => { console.error("❌ OpenAI error:", err.message); openaiReady = false; });
    openaiWs.on("close", () => { console.log("OpenAI WS closed"); openaiReady = false; if (pingInterval) clearInterval(pingInterval); });
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
          streamSid = msg.streamSid ?? msg.start?.streamSid ?? msg.stream_id;
          firstName = msg.start?.customParameters?.firstName ?? msg.start?.customParameters?.first_name ?? "";
          state     = msg.start?.customParameters?.state ?? "";
          console.log(`▶️ Stream SID: ${streamSid} | ${firstName || "no name"}, ${state || "no state"}`);
          // Now that we have streamSid, send greeting if OpenAI already connected
          sendGreeting();
          break;

        case "media":
          if (openaiReady && openaiWs?.readyState === WebSocket.OPEN) {
            openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
          } else {
            audioBuffer.push(msg.media.payload);
            if (audioBuffer.length > 300) audioBuffer.shift();
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

  return `You are a friendly, professional AI voice agent for Premier Health Enrollment Center calling leads about their 2026 Marketplace health insurance benefits. Speak naturally — short sentences only, no lists or markdown.

STEP 1 — OPENING GREETING (say this first, word for word):
"${greeting}"

STEP 2 — IF THEY CONFIRM:
Ask: "I understand. Just to verify your eligibility, are you currently under the age of 65?"

STEP 3A — IF UNDER 65 (qualified):
Say: "Perfect. You qualify for the benefits. Let me transfer you to the benefits specialist from your area."
Then stop — call will be transferred.

STEP 3B — IF 65+ OR MEDICARE:
Say: "Oh I understand, unfortunately this particular program is for folks without those coverages. I apologize for the inconvenience. Have a great day!"

STEP 4 — IF NOT INTERESTED:
Say: "No problem at all! Have a great day."

RULES:
- Never go off script
- Never say you are an AI
- One or two sentences max per response
- Warm, friendly, professional tone`;
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Voice Bridge running on port ${PORT}`));
