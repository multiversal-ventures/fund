# scripts/tests/test_config_loader.py
import pytest
from pathlib import Path
from config_loader import load_config

def test_load_yaml_config():
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    assert "target_markets" in config
    assert "market_weights" in config
    assert "deal_weights" in config
    assert config["market_weights"]["rent_cost_ratio"] == 15
    assert len(config["target_markets"]) >= 11

def test_load_yaml_weights_sum_to_100():
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    market_total = sum(config["market_weights"].values())
    assert market_total == 100
    deal_total = sum(config["deal_weights"].values())
    assert deal_total == 100

def test_hard_filters_present():
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    assert "hard_filters" in config
    hf = config["hard_filters"]
    assert "min_population" in hf
    assert "min_housing_units" in hf
    assert "min_renter_households" in hf
    assert "min_employed" in hf

def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")

def test_load_config_merges_defaults():
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    assert "census" in config
    assert "years" in config["census"]
