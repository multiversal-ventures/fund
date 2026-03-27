#!/usr/bin/env python3
"""DC adjacency Tier 1 pipeline — CBP NAICS 518210, EIA rates, Tavily, scoring."""
from __future__ import annotations

import sys
from pathlib import Path

import click

_scripts = Path(__file__).resolve().parent.parent
_dc = Path(__file__).resolve().parent
for _p in (_scripts, _dc):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from enrich_tavily import enrich_tavily_all_states
from pull_cbp_naics518 import pull_cbp_naics518
from score_dc_markets import run_score_dc


@click.command()
@click.option(
    "--output",
    "output_dir",
    default=str(Path(__file__).resolve().parent.parent.parent / "data"),
    help="Repo data/ directory (contains census/, dc/)",
)
@click.option("--year", default=2023, type=int, help="Census CBP year")
@click.option("--skip-tavily", is_flag=True, help="Skip Tavily HTTP calls (neutral placeholder)")
@click.option("--skip-cbp-pull", is_flag=True, help="Reuse existing dc/cbp_naics518_*.parquet")
def main(output_dir: str, year: int, skip_tavily: bool, skip_cbp_pull: bool):
    """Run pull → enrich → score for DC market layer."""
    out = Path(output_dir)
    dc_dir = out / "dc"
    dc_dir.mkdir(parents=True, exist_ok=True)

    if not skip_cbp_pull:
        pull_cbp_naics518(year, str(dc_dir))
    else:
        print("  Skipping CBP pull (--skip-cbp-pull)")

    enrich_tavily_all_states(str(dc_dir), force_neutral=skip_tavily)

    run_score_dc(str(out))
    print("Done.")


if __name__ == "__main__":
    main()
