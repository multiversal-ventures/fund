/**
 * Cloud Functions — Multiversal fund
 *
 * `dcLocalNews` (live Tavily news for Explorer):
 * 1. Enable Secret Manager API: https://console.cloud.google.com/apis/library/secretmanager.googleapis.com?project=mvv-fund
 * 2. `firebase functions:secrets:set TAVILY_API_KEY` (enter your Tavily API key)
 * 3. `firebase deploy --only functions:dcLocalNews`
 *
 * Keep ALLOWED_EXPLORER_EMAILS in sync with `public/explorer.html` → ALLOWED.
 */
const { onDocumentWritten } = require("firebase-functions/v2/firestore");
const { onRequest } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const { initializeApp } = require("firebase-admin/app");
const { getFirestore } = require("firebase-admin/firestore");
const { getAuth } = require("firebase-admin/auth");

initializeApp();

const REGION = "us-central1";

/** Same allowlist as `public/explorer.html` — keep in sync when adding team members. */
const ALLOWED_EXPLORER_EMAILS = new Set([
  "holly@multiversal.ventures",
  "akshay@multiversal.ventures",
  "kartik@multiversal.ventures",
]);

const tavilyApiKey = defineSecret("TAVILY_API_KEY");

/** CORS for browser fetch from Hosting (Gen2 must allow origin; fixes opaque “Failed to fetch”). */
function applyCors(req, res) {
  const origin = req.headers.origin || "";
  const ok =
    origin === "https://mvv-fund.web.app" ||
    origin === "https://mvv-fund.firebaseapp.com" ||
    /^https:\/\/mvv-fund--[a-z0-9-]+\.web\.app$/i.test(origin) ||
    /^https:\/\/[a-z0-9-]+\.web\.app$/i.test(origin) ||
    /^https:\/\/[a-z0-9-]+\.firebaseapp\.com$/i.test(origin) ||
    /^http:\/\/127\.0\.0\.1(:\d+)?$/i.test(origin) ||
    /^http:\/\/localhost(:\d+)?$/i.test(origin);
  if (ok && origin) {
    res.set("Access-Control-Allow-Origin", origin);
    res.set("Vary", "Origin");
  }
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.set("Access-Control-Max-Age", "7200");
}

function tidySnippet(text, maxLen) {
  if (!text) return "";
  const t = String(text).replace(/\s+/g, " ").trim();
  if (t.length <= maxLen) return t;
  const cut = t.slice(0, maxLen);
  const lastSpace = cut.lastIndexOf(" ");
  return (lastSpace > maxLen * 0.55 ? cut.slice(0, lastSpace) : cut).trim() + "…";
}

function sourceHost(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

/** Split Tavily answer into readable blocks for the UI (no HTML — client escapes). */
function splitSummaryParagraphs(answer, maxParas = 14, maxLen = 520) {
  const raw = String(answer || "").trim();
  if (!raw) return [];
  let blocks = raw
    .split(/\r?\n\s*\r?\n/)
    .map((s) => s.replace(/\s+/g, " ").trim())
    .filter(Boolean);
  if (blocks.length === 1 && blocks[0].length > 400) {
    blocks = blocks[0]
      .split(/(?<=[.!?])\s+(?=[A-Z0-9"'(])/)
      .map((s) => s.trim())
      .filter((s) => s.length > 15);
  }
  return blocks.slice(0, maxParas).map((p) => (p.length > maxLen ? tidySnippet(p, maxLen) : p));
}

async function tavilySearchJson(apiKey, query, searchDepth) {
  const tavilyRes = await fetch("https://api.tavily.com/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      api_key: apiKey,
      query,
      topic: "news",
      days: 21,
      max_results: 12,
      search_depth: searchDepth,
      include_answer: true,
    }),
  });
  const text = await tavilyRes.text();
  return { tavilyRes, text };
}

exports.triggerPipelineRefresh = onDocumentWritten(
  "config/pipeline",
  async (event) => {
    const db = getFirestore();
    const config = event.data.after.data();
    const runId = Date.now().toString();

    await db.collection("config").doc("runs").collection("history").doc(runId).set({
      status: "triggered",
      triggeredAt: new Date().toISOString(),
      config: config,
    });

    const ghToken = process.env.GITHUB_TOKEN;
    if (!ghToken) {
      console.error("GITHUB_TOKEN not set — cannot trigger workflow");
      return;
    }

    const response = await fetch(
      "https://api.github.com/repos/multiversal-ventures/fund/actions/workflows/refresh.yml/dispatches",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${ghToken}`,
          Accept: "application/vnd.github.v3+json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          ref: "main",
          inputs: {
            config_source: "firestore",
            run_id: runId,
          },
        }),
      }
    );

    if (response.ok) {
      console.log(`Triggered workflow for run ${runId}`);
    } else {
      const body = await response.text();
      console.error(`GitHub API error: ${response.status} ${body}`);
      await db.collection("config").doc("runs").collection("history").doc(runId).update({
        status: "trigger_failed",
        error: body,
      });
    }
  }
);

/**
 * POST JSON body: { state: string (2-letter), county?: string, fips?: string }
 * Header: Authorization: Bearer <Firebase ID token>
 * Returns Tavily news results for data-center / hyperscale context in that geography.
 */
exports.dcLocalNews = onRequest(
  {
    region: REGION,
    secrets: [tavilyApiKey],
    invoker: "public",
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (req, res) => {
    applyCors(req, res);
    if (req.method === "OPTIONS") {
      res.status(204).send("");
      return;
    }
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { state, county?, fips? }" });
      return;
    }

    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      res.status(401).json({ error: "Missing Authorization: Bearer <idToken>" });
      return;
    }
    const idToken = authHeader.slice(7);

    let email = "";
    try {
      const decoded = await getAuth().verifyIdToken(idToken);
      email = (decoded.email || "").toLowerCase();
      if (!ALLOWED_EXPLORER_EMAILS.has(email)) {
        res.status(403).json({ error: "Not authorized" });
        return;
      }
    } catch (e) {
      console.warn("verifyIdToken failed", e.message);
      res.status(401).json({ error: "Invalid or expired token" });
      return;
    }

    let body = req.body && typeof req.body === "object" && !Buffer.isBuffer(req.body) ? req.body : {};
    if ((!body || Object.keys(body).length === 0) && req.rawBody) {
      try {
        body = JSON.parse(req.rawBody.toString("utf8"));
      } catch {
        body = {};
      }
    }
    const stateRaw = String(body.state || "").trim().toUpperCase();
    const countyRaw = body.county != null ? String(body.county).trim().slice(0, 120) : "";
    const fipsRaw = body.fips != null ? String(body.fips).trim().slice(0, 12) : "";

    if (!stateRaw || stateRaw.length !== 2 || !/^[A-Z]{2}$/.test(stateRaw)) {
      res.status(400).json({ error: "state must be a 2-letter US state/DC abbreviation" });
      return;
    }

    const year = new Date().getFullYear();
    let query;
    if (countyRaw) {
      const safeCounty = countyRaw.replace(/[\r\n\u0000]/g, " ").slice(0, 100);
      query = `data center OR hyperscale OR cloud campus OR AI infrastructure news "${safeCounty}" county ${stateRaw} ${year - 1} ${year}`;
    } else {
      query = `data center OR hyperscale OR cloud infrastructure development news ${stateRaw} United States ${year - 1} ${year}`;
    }

    const apiKey = tavilyApiKey.value();
    if (!apiKey) {
      res.status(500).json({ error: "Tavily API key not configured (set secret TAVILY_API_KEY)" });
      return;
    }

    try {
      let { tavilyRes, text } = await tavilySearchJson(apiKey, query, "advanced");
      if (!tavilyRes.ok && tavilyRes.status === 400) {
        console.warn("Tavily advanced rejected (400), retrying basic", text.slice(0, 200));
        ({ tavilyRes, text } = await tavilySearchJson(apiKey, query, "basic"));
      }
      if (!tavilyRes.ok) {
        console.error("Tavily HTTP", tavilyRes.status, text.slice(0, 500));
        res.status(502).json({ error: "Tavily request failed", status: tavilyRes.status });
        return;
      }

      let data;
      try {
        data = JSON.parse(text);
      } catch {
        res.status(502).json({ error: "Invalid Tavily response" });
        return;
      }

      const rawResults = Array.isArray(data.results) ? data.results : [];
      const answerRaw = data.answer != null ? String(data.answer) : "";
      const summaryParagraphs = answerRaw ? splitSummaryParagraphs(answerRaw) : [];
      const summary =
        summaryParagraphs.length > 0 ? summaryParagraphs.join("\n\n") : answerRaw ? tidySnippet(answerRaw, 1400) : null;

      const results = rawResults
        .filter((r) => r && r.url)
        .map((r, i) => ({
          rank: i + 1,
          title: tidySnippet(r.title || "", 200) || "Untitled article",
          url: String(r.url),
          source: sourceHost(r.url),
          snippet: tidySnippet(r.content || r.raw_content || "", 520),
          published_date: r.published_date || r.publishedDate || null,
          score: typeof r.score === "number" && Number.isFinite(r.score) ? Math.round(r.score * 1000) / 1000 : null,
        }));

      res.status(200).json({
        ok: true,
        query,
        queryLabel: countyRaw ? `${countyRaw}, ${stateRaw}` : stateRaw,
        state: stateRaw,
        county: countyRaw || null,
        fips: fipsRaw || null,
        fetchedAt: new Date().toISOString(),
        summary,
        summaryParagraphs: summaryParagraphs.length ? summaryParagraphs : null,
        results,
      });
    } catch (e) {
      console.error("dcLocalNews error", e);
      res.status(500).json({ error: "Internal error" });
    }
  }
);
