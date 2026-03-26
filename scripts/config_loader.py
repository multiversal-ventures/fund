# scripts/config_loader.py
"""Load pipeline configuration from YAML file or Firestore."""
import yaml
from pathlib import Path

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.default.yaml"

def load_config(source: str = None) -> dict:
    """Load config from a YAML file path or 'firestore'.
    Args:
        source: Path to YAML file, or 'firestore' to load from Firestore.
                Defaults to config.default.yaml.
    Returns:
        Merged configuration dict.
    """
    if source is None:
        source = str(DEFAULT_CONFIG_PATH)

    if source == "firestore":
        return _load_from_firestore()

    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {source}")

    with open(path) as f:
        config = yaml.safe_load(f)

    return _merge_defaults(config)


def _load_from_firestore() -> dict:
    """Load config from Firestore /config/pipeline document."""
    import firebase_admin
    from firebase_admin import firestore

    if not firebase_admin._apps:
        firebase_admin.initialize_app()

    db = firestore.client()
    doc = db.collection("config").document("pipeline").get()

    if doc.exists:
        return _merge_defaults(doc.to_dict())

    return _merge_defaults({})


def _merge_defaults(config: dict) -> dict:
    """Merge provided config with defaults. Provided values take precedence."""
    with open(DEFAULT_CONFIG_PATH) as f:
        defaults = yaml.safe_load(f)

    merged = defaults.copy()
    for key, value in config.items():
        if isinstance(value, dict) and key in merged and isinstance(merged[key], dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value

    return merged
