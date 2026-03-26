# scripts/tests/test_config_loader.py
import pytest
from pathlib import Path
from config_loader import load_config

def test_load_yaml_config():
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    assert "target_markets" in config
    assert "scoring_weights" in config
    assert config["scoring_weights"]["rent_cost_ratio"] == 30
    assert len(config["target_markets"]) >= 11

def test_load_yaml_weights_sum_to_100():
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    total = sum(config["scoring_weights"].values())
    assert total == 100

def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")

def test_load_config_merges_defaults():
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    assert "census" in config
    assert "years" in config["census"]
