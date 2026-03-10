const express = require("express");
const { WebSocketServer, WebSocket } = require("ws");
const http    = require("http");
const crypto  = require("crypto");

const OPENAI_API_KEY  = process.env.OPENAI_API_KEY;
const TELNYX_API_KEY  = process.env.TELNYX_API_KEY;
if (!OPENAI_API_KEY) { console.error("❌ OPENAI_API_KEY required"); process.exit(1); }
if (!TELNYX_API_KEY)  console.warn("⚠️  TELNYX_API_KEY not set — transfer/hangup will fail");

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

app.get("/", (req, res) => res.json({ status: "Voice Bridge Running ✅", connections: wss.clients.size }));

wss.on("connection", (telnyxWs) => {
  console.log("📲 Telnyx connected");

  let openaiWs      = null;
  let streamSid     = null;
  let callControlId = null;
  let openaiReady   = false;
  let greetingSent  = false;
  let firstName     = "";
  let state         = "";
  let greetingTimeout = null;

  // Loopback prevention — fingerprint outbound audio we send to Telnyx
  const sentAudioHashes = new Set();
  let agentSpeaking = false;
  let reopenAt      = 0;

  function hashChunk(b64) {
    return crypto.createHash("md5").update(b64.substring(0, 32)).digest("hex");
  }

  function callerAudioReady() {
    return !agentSpeaking && Date.now() >= reopenAt;
  }

  function telnyxAction(action, body = {}) {
    if (!callControlId) return;
    return fetch(`https://api.telnyx.com/v2/calls/${callControlId}/actions/${action}`, {
      method: "POST",
      headers: { Authorization: `Bearer ${TELNYX_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).then(r => r.json())
      .then(d => { if (d.errors) console.error(`❌ ${action} error:`, JSON.stringify(d.errors)); else console.log(`✅ ${action} done`); })
      .catch(e => console.error(`❌ ${action} failed:`, e.message));
  }

  function sendGreeting() {
    if (greetingSent || !openaiReady || !streamSid) return;
    greetingSent = true;
    if (greetingTimeout) clearTimeout(greetingTimeout);
    console.log("🗣️ Sending greeting");
    openaiWs.send(JSON.stringify({ type: "response.create" }));
  }

  function connectOpenAI() {
    console.log("🔌 Connecting to OpenAI Realtime...");
    openaiWs = new WebSocket(
      "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17",
      { headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "OpenAI-Beta": "realtime=v1" } }
    );

    const t0 = Date.now();
    openaiWs.on("open", () => {
      console.log(`✅ OpenAI connected in ${Date.now() - t0}ms`);
      openaiReady = true;

      openaiWs.send(JSON.stringify({
        type: "session.update",
        session: {
          turn_detection: {
            type: "server_vad",
            threshold: 0.5,
            prefix_padding_ms: 300,
            silence_duration_ms: 800,
          },
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
    });

    openaiWs.on("message", (data) => {
      try {
        const msg = JSON.parse(data);
        switch (msg.type) {

          case "response.audio.delta":
            agentSpeaking = true;
            if (streamSid && telnyxWs.readyState === WebSocket.OPEN) {
              sentAudioHashes.add(hashChunk(msg.delta));
              if (sentAudioHashes.size > 500) {
                const arr = [...sentAudioHashes];
                arr.slice(0, 250).forEach(h => sentAudioHashes.delete(h));
              }
              telnyxWs.send(JSON.stringify({
                event: "media",
                stream_id: streamSid,
                media: { payload: msg.delta },
              }));
            }
            break;

          case "response.audio.done":
            console.log("✅ Agent finished speaking — listening for caller");
            agentSpeaking = false;
            reopenAt = Date.now() + 1500;
            setTimeout(() => {
              sentAudioHashes.clear();
              if (openaiWs?.readyState === WebSocket.OPEN) {
                openaiWs.send(JSON.stringify({ type: "input_audio_buffer.clear" }));
              }
              console.log("👂 Now listening for caller");
            }, 1500);
            break;

          case "conversation.item.input_audio_transcription.completed":
            if (msg.transcript?.trim()) console.log("👤 Caller:", msg.transcript);
            break;

          case "response.audio_transcript.done": {
            const transcript = msg.transcript ?? "";
            console.log("🤖 Agent:", transcript);
            const t = transcript.toLowerCase();

            if (t.includes("transfer you to") && callControlId) {
              console.log("🔀 Transferring to specialist");
              telnyxAction("transfer", { to: "sip:16194899412@qeCpRfegq5Kefhp.sip.telnyx.com" });
            }

            const isGoodbye = t.includes("have a great day") || t.includes("apologize for the inconvenience");
            if (isGoodbye && callControlId) {
              console.log("📴 Hanging up after goodbye");
              setTimeout(() => telnyxAction("hangup"), 2500);
            }
            break;
          }

          case "session.updated":
            console.log("✅ Session configured");
            break;

          case "error":
            if (msg.error?.code !== "response_cancel_not_active") {
              console.error("❌ OpenAI error:", JSON.stringify(msg.error));
            }
            break;
        }
      } catch (err) { console.error("OpenAI parse error:", err); }
    });

    openaiWs.on("error", (err) => { console.error("❌ OpenAI WS error:", err.message); openaiReady = false; });
    openaiWs.on("close", (code) => { console.log("OpenAI WS closed", code); openaiReady = false; });
  }

  connectOpenAI();

  greetingTimeout = setTimeout(() => {
    if (!greetingSent && openaiReady && streamSid) sendGreeting();
  }, 2000);

  telnyxWs.on("message", (data) => {
    try {
      const msg = JSON.parse(data);
      switch (msg.event) {

        case "connected":
          console.log("✅ Telnyx stream connected");
          break;

        case "start":
          streamSid     = msg.stream_id ?? msg.start?.streamSid;
          callControlId = msg.call_control_id
                        ?? msg.start?.call_control_id
                        ?? msg.start?.customParameters?.callControlId
                        ?? null;
          firstName = msg.start?.customParameters?.firstName ?? msg.start?.customParameters?.first_name ?? "";
          state     = msg.start?.customParameters?.state ?? "";
          console.log(`▶️ SID: ${streamSid} | ccid: ${callControlId}`);
          sendGreeting();
          break;

        case "media": {
          if (!greetingSent || !callerAudioReady() || !openaiReady || openaiWs?.readyState !== WebSocket.OPEN) break;
          const payload = msg.media.payload;
          // Drop packets matching audio we sent — kills loopback
          if (sentAudioHashes.has(hashChunk(payload))) break;
          openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: payload }));
          break;
        }

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
    if (openaiWs?.readyState === WebSocket.OPEN) openaiWs.close();
  }
});

function buildSystemPrompt(firstName, state) {
  const hasName = firstName?.trim().length > 0;
  const greeting = hasName
    ? `Hi, is ${firstName} available? Hi ${firstName}! I'm calling from Premier Health Enrollment Center about your 2026 Marketplace benefits. You currently have insurance through the Marketplace, right?`
    : `Hi there! I'm calling from Premier Health Enrollment Center about your 2026 Marketplace benefits. You currently have insurance through the Marketplace, right?`;

  return `You are a friendly professional AI voice agent for Premier Health Enrollment Center. Short sentences only. Never use lists or markdown. Never say you are an AI.

STRICT RULES:
- Say ONLY the scripted lines below. Nothing else.
- After EVERY question, go completely SILENT. Do not speak again until the caller gives a clear answer.
- A clear answer is: yes, no, right, correct, yep, nope, uh-huh, or any direct response.
- If you hear anything unclear, noise, or silence — keep waiting. Do NOT speak.
- Never repeat a question unless the caller asks you to.

STEP 1 — Say this exactly, then STOP and wait:
"${greeting}"

STEP 2 — Only after clear YES/confirmation to Step 1, say this exactly, then STOP and wait:
"Just to verify your eligibility, are you currently under the age of 65?"

STEP 3A — Only after clear YES (under 65), say this exactly, then STOP:
"Perfect. You qualify for the benefits. Let me transfer you to the benefits specialist from your area."

STEP 3B — Only after NO or mentions Medicare/65+, say this exactly, then STOP:
"Oh I understand, unfortunately this program is for folks without those coverages. I apologize for the inconvenience. Have a great day!"

STEP 4 — If at ANY point they say not interested, stop calling, or wrong number, say this exactly, then STOP:
"No problem at all! Have a great day."

NEVER chain steps together. ONE step. Then SILENCE. Wait for response.`;
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Voice Bridge running on port ${PORT}`));
