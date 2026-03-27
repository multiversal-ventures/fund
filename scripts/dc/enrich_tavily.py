# scripts/dc/enrich_tavily.py
"""State-level web intelligence via Tavily Search API (see https://docs.tavily.com/documentation/agent-skills.md)."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pandas as pd
import requests

TAVILY_URL = "https://api.tavily.com/search"

# Contiguous + AK, HI, DC — same set as Census county pulls
STATE_ABBRS = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY",
]


def _political_score_from_text(blob: str) -> float:
    t = blob.lower()
    pos = sum(1 for k in ("tax incentive", "tax exemption", "abatement", "sales tax", "data center") if k in t)
    neg = sum(1 for k in ("moratorium", "opposition", "blocked", "denied", "lawsuit", "pause") if k in t)
    x = 0.35 + 0.08 * pos - 0.12 * neg
    return float(max(0.0, min(1.0, x)))


def _penalty_from_risk_text(blob: str) -> float:
    t = blob.lower()
    hits = sum(1 for k in ("moratorium", "pause", "opposition", "blocked", "denied") if k in t)
    return float(min(4.0, hits * 1.5))


def fetch_state_intel(
    state_abbr: str,
    api_key: str,
    session: requests.Session,
) -> dict:
    q1 = f"data center tax incentive OR sales tax exemption {state_abbr} 2024 2025"
    q2 = f"data center moratorium OR opposition OR paused {state_abbr} 2024 2025"

    def _post(query: str) -> dict:
        r = session.post(
            TAVILY_URL,
            json={
                "api_key": api_key,
                "query": query,
                "search_depth": "basic",
                "max_results": 6,
            },
            timeout=60,
        )
        r.raise_for_status()
        return r.json()

    out1 = _post(q1)
    time.sleep(0.15)
    out2 = _post(q2)

    parts = []
    for o in (out1, out2):
        for res in o.get("results") or []:
            parts.append(res.get("content") or "")
            parts.append(res.get("title") or "")
    blob = " ".join(parts)

    return {
        "state_abbr": state_abbr,
        "tavily_political_score": _political_score_from_text(blob),
        "tavily_penalty": _penalty_from_risk_text(blob),
        "tavily_snippet_digest": blob[:1200],
        "tavily_sources_json": json.dumps(
            [{"url": r.get("url"), "title": r.get("title")} for o in (out1, out2) for r in (o.get("results") or [])][:12]
        ),
    }


def enrich_tavily_all_states(
    output_dir: str,
    api_key: str | None = None,
    *,
    force_neutral: bool = False,
) -> Path | None:
    key = None if force_neutral else (api_key or os.environ.get("TAVILY_API_KEY"))
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dc_tavily_state.parquet"

    if not key:
        df = pd.DataFrame(
            {
                "state_abbr": STATE_ABBRS,
                "tavily_political_score": [0.5] * len(STATE_ABBRS),
                "tavily_penalty": [0.0] * len(STATE_ABBRS),
                "tavily_snippet_digest": [""] * len(STATE_ABBRS),
                "tavily_sources_json": ["[]"] * len(STATE_ABBRS),
            }
        )
        df.to_parquet(out_path, index=False)
        print(f"  No TAVILY_API_KEY — wrote neutral placeholder → {out_path}")
        return out_path

    rows = []
    sess = requests.Session()
    for i, st in enumerate(STATE_ABBRS):
        try:
            rows.append(fetch_state_intel(st, key, sess))
        except Exception as e:
            rows.append(
                {
                    "state_abbr": st,
                    "tavily_political_score": 0.5,
                    "tavily_penalty": 0.0,
                    "tavily_snippet_digest": f"error: {e}",
                    "tavily_sources_json": "[]",
                }
            )
        if (i + 1) % 10 == 0:
            print(f"    Tavily {i + 1}/{len(STATE_ABBRS)} states")
        time.sleep(0.15)

    df = pd.DataFrame(rows)
    df.to_parquet(out_path, index=False)
    print(f"  Wrote Tavily intel → {out_path}")
    return out_path
