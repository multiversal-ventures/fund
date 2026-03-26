const { onDocumentWritten } = require("firebase-functions/v2/firestore");
const { initializeApp } = require("firebase-admin/app");
const { getFirestore } = require("firebase-admin/firestore");

initializeApp();

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
