// =============================================================================
// Voice Bridge Server — Railway.app
// Bridges Telnyx audio stream ↔ OpenAI Realtime API
// =============================================================================

const express = require("express");
const { WebSocketServer, WebSocket } = require("ws");
const http = require("http");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
if (!OPENAI_API_KEY) { console.error("❌ OPENAI_API_KEY required"); process.exit(1); }

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

app.get("/", (req, res) => res.json({ status: "Voice Bridge Running ✅", connections: wss.clients.size }));

wss.on("connection", (telnyxWs, req) => {
  console.log("📲 Telnyx connected");

  let openaiWs     = null;
  let streamSid    = null;
  let pingInterval = null;
  let openaiReady  = false;
  let firstName    = "";
  let state        = "";

  // Buffer audio that arrives before OpenAI is ready
  const audioBuffer = [];

  function connectOpenAI() {
    console.log("🔌 Connecting to OpenAI Realtime...");

    openaiWs = new WebSocket(
      "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01",
      {
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          "OpenAI-Beta": "realtime=v1",
        },
      }
    );

    openaiWs.on("open", () => {
      console.log("✅ OpenAI Realtime connected");
      openaiReady = true;

      const prompt = buildSystemPrompt(firstName, state);

      openaiWs.send(JSON.stringify({
        type: "session.update",
        session: {
          turn_detection: {
            type: "server_vad",
            threshold: 0.5,
            prefix_padding_ms: 300,
            silence_duration_ms: 500,
          },
          input_audio_format: "g711_ulaw",
          output_audio_format: "g711_ulaw",
          input_audio_transcription: { model: "whisper-1" },
          voice: "shimmer",
          instructions: prompt,
          modalities: ["text", "audio"],
          temperature: 0.7,
        },
      }));

      // Flush any buffered audio
      if (audioBuffer.length > 0) {
        console.log(`📦 Flushing ${audioBuffer.length} buffered audio chunks`);
        for (const chunk of audioBuffer) {
          openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: chunk }));
        }
        audioBuffer.length = 0;
      }

      // Agent speaks first
      openaiWs.send(JSON.stringify({
        type: "conversation.item.create",
        item: {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "The call just connected. Say your opening greeting now exactly as instructed." }],
        },
      }));
      openaiWs.send(JSON.stringify({ type: "response.create" }));

      pingInterval = setInterval(() => {
        if (openaiWs?.readyState === WebSocket.OPEN) {
          openaiWs.send(JSON.stringify({ type: "ping" }));
        }
      }, 30_000);
    });

    openaiWs.on("message", (data) => {
      try {
        const msg = JSON.parse(data);
        switch (msg.type) {

          case "response.audio.delta":
            if (telnyxWs.readyState === WebSocket.OPEN) {
              if (streamSid) {
                // Send audio to Telnyx
                telnyxWs.send(JSON.stringify({
                  event: "media",
                  streamSid,
                  media: { payload: msg.delta },
                }));
              } else {
                console.warn("⚠️ Got audio delta but streamSid not set yet — buffering");
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
      } catch (err) {
        console.error("OpenAI message parse error:", err);
      }
    });

    openaiWs.on("error", (err) => { console.error("❌ OpenAI WS error:", err.message); openaiReady = false; });
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
          break;

        case "media":
          if (openaiReady && openaiWs?.readyState === WebSocket.OPEN) {
            openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
          } else {
            // Buffer audio until OpenAI is ready
            audioBuffer.push(msg.media.payload);
            if (audioBuffer.length > 200) audioBuffer.shift(); // max 200 chunks
          }
          break;

        case "stop":
          console.log("⏹️ Stream stopped");
          cleanup();
          break;
      }
    } catch (err) {
      console.error("Telnyx message parse error:", err);
    }
  });

  telnyxWs.on("close", () => { console.log("Telnyx WS closed"); cleanup(); });
  telnyxWs.on("error", (err) => console.error("❌ Telnyx WS error:", err.message));

  function cleanup() {
    if (pingInterval) clearInterval(pingInterval);
    if (openaiWs?.readyState === WebSocket.OPEN) openaiWs.close();
  }
});

// =============================================================================
// SYSTEM PROMPT
// =============================================================================

function buildSystemPrompt(firstName, state) {
  const hasName = firstName && firstName.trim().length > 0;
  const openingGreeting = hasName
    ? `Hello? ... Hi ${firstName}, I'm calling from Premier Health Enrollment Center, ${state}. This short call is about your newly updated Marketplace benefits for 2026, and I believe you already have insurance through the Marketplace, right?`
    : `Hello? ... Hi, I'm calling from Premier Health Enrollment Center, ${state || "your area"}. This short call is about your newly updated Marketplace benefits for 2026, and I believe you already have insurance through the Marketplace, right?`;

  return `You are a friendly, professional AI voice agent for Premier Health Enrollment Center calling leads about their 2026 Marketplace health insurance benefits. Speak naturally and conversationally — short sentences only, no lists or markdown.

Follow this exact script flow:

STEP 1 — OPENING GREETING (say this first, word for word):
"${openingGreeting}"

STEP 2 — IF THEY CONFIRM (say yes, correct, right, uh huh, etc.):
Ask: "I understand. Just to verify your eligibility, are you currently under the age of 65?"

STEP 3A — IF UNDER 65 (qualified):
Say: "Perfect. You qualify for the benefits. Let me transfer you to the benefits specialist from your area."
Then say nothing more — the call will be transferred.

STEP 3B — IF 65 OR OLDER / ON MEDICARE (disqualified):
Say: "Oh I understand, unfortunately this particular program is for folks without those coverages. I apologize for the inconvenience. Have a great day!"
Then end the call politely.

STEP 4 — IF NOT INTERESTED / DECLINES:
Say: "No problem at all! Have a great day."
Then end the call.

RULES:
- Never go off script
- Never mention you are an AI
- Keep responses very short — one or two sentences max
- If they ask a question you cannot answer, say "Let me have a specialist help you with that" and proceed to transfer
- Never repeat yourself
- Speak in a warm, friendly, professional tone`;
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Voice Bridge running on port ${PORT}`));
