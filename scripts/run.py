#!/usr/bin/env python3
# scripts/run.py
"""CLI entry point for the fund data pipeline."""
import os
import subprocess
import sys
import click
from pathlib import Path

from config_loader import load_config
from pull_census import pull_census, fetch_occupation_data
from pull_bls import pull_bls
from pull_hud import pull_hud
from pull_permits import pull_permits
from pull_cbp import pull_cbp
from sensitivity import run_monte_carlo
from score import run_scoring
from upload import upload_all

DEFAULT_OUTPUT = str(Path(__file__).parent.parent / "data")


@click.command()
@click.option("--all", "run_all", is_flag=True, help="Run entire pipeline")
@click.option("--census", is_flag=True, help="Pull Census ACS data")
@click.option("--bls", is_flag=True, help="Pull BLS OEWS data")
@click.option("--hud", is_flag=True, help="Pull HUD FHA + USPS data")
@click.option("--permits", is_flag=True, help="Pull building permits data")
@click.option("--cbp", is_flag=True, help="Pull County Business Patterns / HHI data")
@click.option("--sensitivity", "run_sensitivity", is_flag=True, help="Run Monte Carlo sensitivity analysis")
@click.option("--score", "run_score", is_flag=True, help="Run scoring")
@click.option("--upload", "run_upload", is_flag=True, help="Upload to Firebase Storage")
@click.option("--dc", "run_dc", is_flag=True, help="Run data center adjacency Tier 1 pipeline (needs census/acs_*.parquet)")
@click.option(
    "--dc-skip-tavily",
    "dc_skip_tavily",
    is_flag=True,
    help="Skip Tavily API calls for DC enrichment (neutral state intel)",
)
@click.option("--config", "config_source", default=None, help="Config file path or 'firestore'")
@click.option("--local-only", is_flag=True, help="Skip upload, output locally only")
@click.option("--output", default=DEFAULT_OUTPUT, help="Local output directory")
def main(
    run_all,
    census,
    bls,
    hud,
    permits,
    cbp,
    run_sensitivity,
    run_score,
    run_upload,
    run_dc,
    dc_skip_tavily,
    config_source,
    local_only,
    output,
):
    """Fund data pipeline — pull, score, and upload multifamily property data."""
    config = load_config(config_source)
    os.makedirs(output, exist_ok=True)

    if run_all or census:
        census_dir = str(Path(output) / "census")
        os.makedirs(census_dir, exist_ok=True)
        pull_census(
            years=config["census"]["years"],
            output_dir=census_dir,
            api_key=os.environ.get("CENSUS_API_KEY"),
        )

    if run_all or bls:
        bls_dir = str(Path(output) / "bls")
        os.makedirs(bls_dir, exist_ok=True)
        bls_years = config.get("bls", {}).get("years", config["census"]["years"])
        pull_bls(years=bls_years, output_dir=bls_dir)

    if run_all or hud:
        hud_dir = str(Path(output) / "hud")
        os.makedirs(hud_dir, exist_ok=True)
        pull_hud(output_dir=hud_dir)

    if run_all or permits:
        permits_dir = str(Path(output) / "permits")
        os.makedirs(permits_dir, exist_ok=True)
        pull_permits(
            years=config["census"]["years"],
            output_dir=permits_dir,
            api_key=os.environ.get("CENSUS_API_KEY"),
        )

    if run_all or cbp:
        cbp_dir = str(Path(output) / "cbp")
        os.makedirs(cbp_dir, exist_ok=True)
        pull_cbp(
            years=config["census"]["years"],
            output_dir=cbp_dir,
            api_key=os.environ.get("CENSUS_API_KEY"),
        )

    if run_all or run_score:
        scored_dir = str(Path(output) / "scored")
        os.makedirs(scored_dir, exist_ok=True)
        run_scoring(
            data_dir=output,
            config=config,
            output_path=str(Path(scored_dir) / "properties.parquet"),
        )

    if run_all or run_sensitivity:
        scored_path = str(Path(output) / "scored" / "properties.parquet")
        if Path(scored_path).exists():
            import pandas as pd
            scored = pd.read_parquet(scored_path)
            scored = run_monte_carlo(scored, config)
            scored.to_parquet(scored_path, index=False)
            print(f"  Monte Carlo sensitivity added to {scored_path}")

    if run_all or run_dc:
        dc_script = Path(__file__).resolve().parent / "dc" / "run_dc_pipeline.py"
        cmd = [sys.executable, str(dc_script), "--output", output]
        if dc_skip_tavily:
            cmd.append("--skip-tavily")
        print("DC adjacency pipeline...")
        subprocess.run(cmd, check=True)

    if (run_all or run_upload) and not local_only:
        upload_all(data_dir=output, config=config)

    if local_only:
        print(f"\nLocal-only mode — output at {output}/")

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
