// =============================================================================
// AI AMD Server — Railway
// Exact same logic as VICIdial server.js, adapted for Telnyx ulaw streaming
//
// TWO modes:
//   1. POST /analyze  — receives base64 audio, returns verdict (same as VICIdial)
//   2. WS  /stream    — Telnyx streams audio live, buffers 2.5s, returns verdict
//                       via POST callback to Supabase
// =============================================================================

const express   = require("express");
const WebSocket = require("ws");
const http      = require("http");
const fetch     = require("node-fetch");

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server, path: "/stream" });

app.use(express.json({ limit: "50mb" }));

const PORT          = process.env.PORT || 8084;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// =============================================================================
// CORE: analyzeAudio — EXACT same as VICIdial server.js
// Takes base64 ulaw audio, sends to OpenAI Realtime, returns verdict
// =============================================================================
async function analyzeAudio(callId, b64Audio) {
  return new Promise((resolve) => {
    console.log(`🔍 [${callId}] Analyzing audio (${b64Audio.length} b64 chars)`);

    let openaiWs    = null;
    let verdictSent = false;
    let transcript  = "";

    function done(verdict, reason) {
      if (verdictSent) return;
      verdictSent = true;
      console.log(`🧠 [${callId}] VERDICT: ${verdict.toUpperCase()} — ${reason}`);
      try { openaiWs?.close(); } catch (_) {}
      resolve({ verdict, reason });
    }

    // 5 second max — same as VICIdial
    const timer = setTimeout(() => done("human", "timeout — defaulting human"), 5000);

    openaiWs = new WebSocket(
      "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17",
      {
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          "OpenAI-Beta": "realtime=v1",
        },
      }
    );

    openaiWs.on("open", () => {
      // Step 1: Configure — text only, g711_ulaw input, manual commit, temp 0.6
      openaiWs.send(JSON.stringify({
        type: "session.update",
        session: {
          modalities: ["text"],
          input_audio_format: "g711_ulaw",
          input_audio_transcription: { model: "whisper-1" },
          turn_detection: null, // manual commit — same as VICIdial
          instructions: `Listen to this audio and respond with ONLY one word: HUMAN or MACHINE.
MACHINE = voicemail greeting, recorded message, automated system, "please leave a message", beep, robot voice, "you have reached", "not available", "cannot take your call", long formal sentence
HUMAN = "hello", "yes", "hi", "hey", short natural response, real person
Respond with exactly one word: HUMAN or MACHINE`,
          temperature: 0.6,
          max_response_output_tokens: 5,
        },
      }));

      // Step 2: Push audio — EXACT same as VICIdial
      openaiWs.send(JSON.stringify({
        type: "input_audio_buffer.append",
        audio: b64Audio,
      }));

      // Step 3: Commit — triggers transcription + response
      openaiWs.send(JSON.stringify({ type: "input_audio_buffer.commit" }));

      // Step 4: Request response
      openaiWs.send(JSON.stringify({ type: "response.create" }));
    });

    openaiWs.on("message", (data) => {
      try {
        const msg = JSON.parse(data);

        // Live transcription — catch machine phrases immediately
        if (msg.type === "conversation.item.input_audio_transcription.completed") {
          const t = (msg.transcript || "").toLowerCase();
          transcript += " " + t;
          console.log(`👂 [${callId}] Heard: "${t}"`);

          const machineWords = [
            "leave a message", "after the tone", "after the beep", "not available",
            "cannot take your call", "voicemail", "please record", "at the tone",
            "you have reached", "try your call again", "mailbox is full", "google voice",
            "press 1", "for english", "disconnected", "not in service", "no longer",
            "please leave", "record your message", "hang up or press", "currently unavailable",
            "is not available", "has a voicemail", "set up yet", "reach me at",
          ];
          const humanWords = [
            "hello", "hi ", "hey ", "yes", "yeah", "yep", "speaking",
            "this is", "who is", "hold on", "sure", "okay", "what", "uh", "huh",
          ];

          if (machineWords.some(w => transcript.includes(w))) { clearTimeout(timer); return done("machine", `machine phrase: "${t}"`); }
          if (humanWords.some(w => transcript.includes(w)))   { clearTimeout(timer); return done("human",   `human phrase: "${t}"`); }
        }

        // AI direct verdict — same as VICIdial response.text.done
        if (msg.type === "response.text.done") {
          clearTimeout(timer);
          const t = (msg.text || "").toLowerCase().trim();
          console.log(`🤖 [${callId}] AI says: "${t}"`);
          if (t.includes("machine")) return done("machine", "AI classified as machine");
          if (t.includes("human"))   return done("human",   "AI classified as human");
          return done("human", `unclear: "${t}"`);
        }

        if (msg.type === "response.audio_transcript.done") {
          clearTimeout(timer);
          const t = (msg.transcript || "").toLowerCase().trim();
          if (t.includes("machine")) return done("machine", "AI transcript: machine");
          if (t.includes("human"))   return done("human",   "AI transcript: human");
        }

        if (msg.type === "error") {
          console.error(`❌ [${callId}] OpenAI error:`, msg.error?.message);
          clearTimeout(timer);
          done("human", "OpenAI error — defaulting human");
        }
      } catch (e) { console.error(`Parse error [${callId}]:`, e); }
    });

    openaiWs.on("error", (e) => { clearTimeout(timer); done("human", `WS error: ${e.message}`); });
    openaiWs.on("close", ()  => { if (!verdictSent) { clearTimeout(timer); done("human", "WS closed early"); } });
  });
}

// =============================================================================
// ROUTE 1: POST /analyze
// Same interface as VICIdial server — Supabase sends base64 audio, gets verdict
// Body: { call_id, audio (base64 ulaw), callback_url (optional) }
// =============================================================================
app.post("/analyze", async (req, res) => {
  const { call_id, audio, callback_url } = req.body;

  if (!audio) {
    return res.json({ verdict: "human", reason: "no audio provided" });
  }

  if (!OPENAI_API_KEY) {
    return res.json({ verdict: "human", reason: "no OPENAI_API_KEY configured" });
  }

  try {
    const result = await analyzeAudio(call_id || "unknown", audio);

    // If callback_url provided, also POST the result there (async mode)
    if (callback_url) {
      fetch(callback_url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ call_id, ...result }),
      }).catch(e => console.error("Callback failed:", e));
    }

    res.json(result);
  } catch (err) {
    console.error("analyzeAudio error:", err);
    res.json({ verdict: "human", reason: "error — defaulting human" });
  }
});

// =============================================================================
// ROUTE 2: WebSocket /stream
// Telnyx streams audio live here — we buffer 2.5s then analyze
// After verdict, POST result to callback_url so Supabase can act on it
// =============================================================================
wss.on("connection", (ws, req) => {
  console.log("🔌 New Telnyx stream connection");

  let callControlId = null;
  let callbackUrl   = null;
  let chunks        = [];
  let analyzing     = false;
  let analysisTimer = null;

  ws.on("message", async (data) => {
    try {
      const msg = JSON.parse(data);

      if (msg.event === "connected") {
        console.log("✅ Stream connected");

      } else if (msg.event === "start") {
        // Extract call_control_id and callback_url from Telnyx start event
        callControlId = msg.start?.call_control_id
          ?? msg.start?.customParameters?.call_control_id
          ?? msg.call_control_id
          ?? null;
        callbackUrl = msg.start?.customParameters?.callback_url
          ?? msg.start?.callback_url
          ?? null;

        console.log(`▶️  Stream started — ccId: ${callControlId}, callback: ${callbackUrl}`);

        // Auto-analyze after 2.5s of audio — same window as VICIdial MixMonitor
        analysisTimer = setTimeout(async () => {
          if (analyzing || chunks.length === 0) return;
          analyzing = true;

          const combinedAudio = chunks.join("");
          chunks = [];
          console.log(`⏱️  [${callControlId}] 2.5s elapsed — analyzing ${combinedAudio.length} b64 chars`);

          const result = await analyzeAudio(callControlId || "stream", combinedAudio);

          // POST verdict back to Supabase so it can hangup/continue
          if (callbackUrl) {
            try {
              await fetch(callbackUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  call_control_id: callControlId,
                  verdict: result.verdict,
                  reason: result.reason,
                }),
              });
              console.log(`📤 [${callControlId}] Verdict sent to callback: ${result.verdict}`);
            } catch (e) {
              console.error(`❌ [${callControlId}] Callback POST failed:`, e.message);
            }
          }

          ws.close();
        }, 2500);

      } else if (msg.event === "media") {
        // Buffer audio chunks — same as VICIdial PHP collecting the WAV file
        if (!analyzing) {
          chunks.push(msg.media?.payload ?? "");
        }

      } else if (msg.event === "stop") {
        console.log(`⏹️  Stream stopped for ${callControlId}`);
        clearTimeout(analysisTimer);

        // Analyze whatever we collected if not already done
        if (!analyzing && chunks.length > 0) {
          analyzing = true;
          const combinedAudio = chunks.join("");
          chunks = [];
          const result = await analyzeAudio(callControlId || "stream", combinedAudio);

          if (callbackUrl) {
            fetch(callbackUrl, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                call_control_id: callControlId,
                verdict: result.verdict,
                reason: result.reason,
              }),
            }).catch(e => console.error("Callback failed:", e));
          }
        }
      }
    } catch (e) { console.error("Stream message parse error:", e); }
  });

  ws.on("error", (e) => console.error("Stream WS error:", e));
  ws.on("close", () => { clearTimeout(analysisTimer); console.log(`🔌 Stream closed for ${callControlId}`); });
});

// =============================================================================
// HEALTH CHECK
// =============================================================================
app.get("/", (req, res) => {
  res.json({
    status: "AI AMD Server Running 🧠",
    openai_configured: !!OPENAI_API_KEY,
    endpoints: {
      "POST /analyze": "Send base64 audio, get { verdict, reason }",
      "WS   /stream":  "Telnyx streams audio live, verdict POSTed to callback_url",
    },
  });
});

server.listen(PORT, () => {
  console.log(`🚀 AI AMD running on port ${PORT}`);
  console.log(`   OpenAI: ${OPENAI_API_KEY ? "✅ configured" : "❌ MISSING"}`);
});
