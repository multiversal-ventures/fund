#!/usr/bin/env python3
# scripts/run.py
"""CLI entry point for the fund data pipeline."""
import os
import click
from pathlib import Path

from config_loader import load_config
from pull_census import pull_census, fetch_occupation_data
from pull_bls import pull_bls
from pull_hud import pull_hud
from score import run_scoring
from upload import upload_all

DEFAULT_OUTPUT = str(Path(__file__).parent.parent / "data")


@click.command()
@click.option("--all", "run_all", is_flag=True, help="Run entire pipeline")
@click.option("--census", is_flag=True, help="Pull Census ACS data")
@click.option("--bls", is_flag=True, help="Pull BLS OEWS data")
@click.option("--hud", is_flag=True, help="Pull HUD FHA + USPS data")
@click.option("--score", "run_score", is_flag=True, help="Run scoring")
@click.option("--upload", "run_upload", is_flag=True, help="Upload to Firebase Storage")
@click.option("--config", "config_source", default=None, help="Config file path or 'firestore'")
@click.option("--local-only", is_flag=True, help="Skip upload, output locally only")
@click.option("--output", default=DEFAULT_OUTPUT, help="Local output directory")
def main(run_all, census, bls, hud, run_score, run_upload, config_source, local_only, output):
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

    if run_all or run_score:
        scored_dir = str(Path(output) / "scored")
        os.makedirs(scored_dir, exist_ok=True)
        run_scoring(
            data_dir=output,
            config=config,
            output_path=str(Path(scored_dir) / "properties.parquet"),
        )

    if (run_all or run_upload) and not local_only:
        upload_all(data_dir=output, config=config)

    if local_only:
        print(f"\nLocal-only mode — output at {output}/")

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
