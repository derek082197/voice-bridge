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
  let callControlId = null;
  let pingInterval  = null;
  let openaiReady   = false;
  let greetingSent  = false;
  let firstName     = "";
  let state         = "";



  let greetingTimeout = null;
  let agentSpeaking    = false;
  let reopenAt         = 0;

  function callerAudioReady() {
    return !agentSpeaking && Date.now() >= reopenAt;
  }
  const audioBuffer   = []; // collect all chunks, send as one

  function sendGreeting() {
    if (greetingSent || !openaiReady || !streamSid) return;
    greetingSent = true;
    if (greetingTimeout) clearTimeout(greetingTimeout);
    console.log("🗣️ Sending greeting");
    // Trigger agent to speak first — single clean instruction
    openaiWs.send(JSON.stringify({ type: "response.create" }));
  }

  // Safety net: force greeting after 3s even if something is slow
  greetingTimeout = setTimeout(() => {
    if (!greetingSent && openaiReady && streamSid) {
      console.log("⏰ Forcing greeting after timeout");
      sendGreeting();
    }
  }, 1000);

  function connectOpenAI() {
    console.log("🔌 Connecting to OpenAI Realtime...");

    openaiWs = new WebSocket(
      "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17",
      { headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "OpenAI-Beta": "realtime=v1" } }
    );

    const openaiConnectTime = Date.now();
    openaiWs.on("open", () => {
      console.log(`✅ OpenAI connected in ${Date.now() - openaiConnectTime}ms`);
      openaiReady = true;

      openaiWs.send(JSON.stringify({
        type: "session.update",
        session: {
          turn_detection: { type: "server_vad", threshold: 0.6, prefix_padding_ms: 200, silence_duration_ms: 1200 },
          input_audio_format: "g711_ulaw",
          output_audio_format: "g711_ulaw",
          input_audio_transcription: { model: "whisper-1" },
          voice: "shimmer",
          instructions: buildSystemPrompt(firstName, state),
          modalities: ["text", "audio"],
          temperature: 0.7,
          max_response_output_tokens: 500,
        },
      }));

      sendGreeting();

      // Removed ping — OpenAI Realtime does not support ping type
    });

    openaiWs.on("message", (data) => {
      try {
        const msg = JSON.parse(data);
        switch (msg.type) {



          case "response.audio.delta":
            agentSpeaking = true;
            // Stream audio immediately as it arrives
            if (streamSid && telnyxWs.readyState === WebSocket.OPEN) {
              telnyxWs.send(JSON.stringify({
                event: "media",
                stream_id: streamSid,
                media: { payload: msg.delta },
              }));
            }
            break;

          case "response.audio.done": {
            audioBuffer.length = 0;
            console.log("✅ Agent finished speaking — listening for caller");
            agentSpeaking = false;
            reopenAt = Date.now() + 1000; // block caller audio for 1s after agent stops
            setTimeout(() => {
              if (openaiWs?.readyState === WebSocket.OPEN) {
                openaiWs.send(JSON.stringify({ type: "input_audio_buffer.clear" }));
              }
              console.log("👂 Now listening for caller");
            }, 1000);
            break;
          }

          case "conversation.item.input_audio_transcription.completed":
            console.log("👤 Caller:", msg.transcript);
            break;

          case "response.audio_transcript.done":
            console.log("🤖 Agent:", msg.transcript);
            const t = msg.transcript?.toLowerCase() ?? "";

            // Transfer to specialist
            if (t.includes("transfer you to") && callControlId) {
              console.log("🔀 Triggering transfer to specialist");
              fetch(`https://api.telnyx.com/v2/calls/${callControlId}/actions/transfer`, {
                method: "POST",
                headers: {
                  Authorization: `Bearer ${process.env.TELNYX_API_KEY}`,
                  "Content-Type": "application/json",
                },
                body: JSON.stringify({
                  to: "sip:16194899412@qeCpRfegq5Kefhp.sip.telnyx.com",
                }),
              }).then(r => r.json()).then(d => console.log("✅ Transfer result:", JSON.stringify(d)))
                .catch(e => console.error("❌ Transfer error:", e));
            }

            // Hangup after goodbye lines
            const isGoodbye = t.includes("have a great day") || t.includes("apologize for the inconvenience") || t.includes("no problem at all");
            if (isGoodbye && callControlId) {
              console.log("📴 Hanging up after goodbye");
              setTimeout(() => {
                fetch(`https://api.telnyx.com/v2/calls/${callControlId}/actions/hangup`, {
                  method: "POST",
                  headers: {
                    Authorization: `Bearer ${process.env.TELNYX_API_KEY}`,
                    "Content-Type": "application/json",
                  },
                  body: JSON.stringify({}),
                }).then(() => console.log("✅ Call hung up"))
                  .catch(e => console.error("❌ Hangup error:", e));
              }, 2000); // Wait 2s so goodbye audio finishes playing
            }
            break;

          case "session.updated":
            console.log("✅ Session configured");
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

  // Pre-warm OpenAI immediately on WS connection — don't wait for "connected" event
  connectOpenAI();

  telnyxWs.on("message", (data) => {
    try {
      const msg = JSON.parse(data);
      switch (msg.event) {
        case "connected":
          console.log("✅ Telnyx stream connected");
          break;

        case "start":
          streamSid      = msg.stream_id ?? msg.streamSid ?? msg.start?.streamSid;
          callControlId  = msg.call_control_id
                         ?? msg.start?.call_control_id
                         ?? msg.start?.customParameters?.callControlId
                         ?? msg.start?.headers?.find(h => h.name === "callControlId")?.value
                         ?? null;
          firstName      = msg.start?.customParameters?.firstName ?? msg.start?.customParameters?.first_name ?? "";
          state          = msg.start?.customParameters?.state ?? "";
          console.log(`▶️ SID: ${streamSid} | "${firstName}" "${state}" | ccid: ${callControlId}`);
          console.log("📋 Start payload:", JSON.stringify(msg.start ?? msg).substring(0, 400));

          // Send silence packets every 200ms to keep call alive while OpenAI loads
          // 20ms of silence at 8kHz mulaw = 160 bytes of 0xFF (mulaw silence value)
          const silencePacket = Buffer.alloc(160, 0xFF).toString("base64");
          const silenceInterval = setInterval(() => {
            if (greetingSent) {
              clearInterval(silenceInterval); // Stop once agent starts speaking
              return;
            }
            if (telnyxWs.readyState === WebSocket.OPEN) {
              telnyxWs.send(JSON.stringify({
                event: "media",
                stream_id: streamSid,
                media: { payload: silencePacket },
              }));
            }
          }, 200);

          sendGreeting();
          break;

        case "media":
          // Only forward caller audio when agent is NOT speaking — blocks loopback
          if (greetingSent && callerAudioReady() && openaiReady && openaiWs?.readyState === WebSocket.OPEN) {
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
    if (pingInterval)  clearInterval(pingInterval);
    if (openaiWs?.readyState === WebSocket.OPEN) openaiWs.close();
  }
});

function buildSystemPrompt(firstName, state) {
  const hasName = firstName && firstName.trim().length > 0;
  const greeting = hasName
    ? `Hi, is ${firstName} available? Hi ${firstName}! I'm calling from Premier Health Enrollment Center about your 2026 Marketplace benefits. You currently have insurance through the Marketplace, right?`
    : `Hi there! I'm calling from Premier Health Enrollment Center about your 2026 Marketplace benefits. You currently have insurance through the Marketplace, right?`;

  return `You are a friendly, professional AI voice agent for Premier Health Enrollment Center. Speak naturally — short sentences only, never use lists or markdown.

YOU MUST FOLLOW THESE RULES STRICTLY:
- Ask ONE question at a time. STOP TALKING after each question. Wait silently for the caller to respond.
- Do NOT proceed to the next step until you hear a clear answer (yes/no/confirm/deny).
- If you hear silence, background noise, or unclear sounds — do NOT respond. Keep waiting.
- Never skip steps. Never assume an answer. Never repeat yourself unless asked.
- Never say you are an AI.

STEP 1 — OPENING (say word for word, then STOP and wait):
"${greeting}"
→ Wait for caller to say yes, correct, right, uh-huh, or similar confirmation.
→ If they say not interested / wrong number / no: say "No problem at all! Have a great day." then STOP.

STEP 2 — ONLY after they confirm marketplace insurance (say word for word, then STOP and wait):
"Just to verify your eligibility, are you currently under the age of 65?"
→ Wait for a clear yes or no answer.
→ If unclear or no response: wait silently. Do NOT proceed.

STEP 3A — ONLY if they confirm under 65 (say word for word, then STOP):
"Perfect. You qualify for the benefits. Let me transfer you to the benefits specialist from your area."
Then stop completely — do not say anything else.

STEP 3B — ONLY if they say 65+ or mention Medicare (say word for word, then STOP):
"Oh I understand, unfortunately this particular program is for folks without those coverages. I apologize for the inconvenience. Have a great day!"
Then stop completely.

STEP 4 — If at any point they say not interested / stop calling / wrong number:
"No problem at all! Have a great day."
Then stop completely.

CRITICAL: After each question, go SILENT and wait. Never chain questions together. Never fill silence with more talking.`;
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Voice Bridge running on port ${PORT}`));
