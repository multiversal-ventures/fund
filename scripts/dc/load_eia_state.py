# scripts/dc/load_eia_state.py
"""State-level industrial electricity prices (¢/kWh) — bundled CSV until EIA API wired."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

EIA_CSV = Path(__file__).resolve().parent / "eia_state_industrial_2023.csv"


def load_eia_state_industrial(csv_path: Path | None = None) -> pd.DataFrame:
    """Columns: state_abbr, industrial_cents_kwh."""
    p = csv_path or EIA_CSV
    df = pd.read_csv(p)
    if "industrial_cents_kwh" not in df.columns:
        raise ValueError(f"Expected industrial_cents_kwh in {p}")
    df = df[["state_abbr", "industrial_cents_kwh"]].copy()
    df["industrial_cents_kwh"] = pd.to_numeric(df["industrial_cents_kwh"], errors="coerce")
    return df
