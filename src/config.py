from pathlib import Path
import os
import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = ROOT_DIR / "config" / "sources.yml"

def load_config(path=None):
    if path is None:
        path = DEFAULT_CONFIG_PATH

    path = Path(path)

    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        
    db_url = config["defaults"].get("db_url")
    if isinstance(db_url, str):
        config["defaults"]["db_url"] = os.path.expandvars(db_url)

    return config
CONFIG = load_config()

def get_source_config(name: str) -> dict:
    for source in CONFIG.get("sources", []):
        if source.get("name") == name:
            return source
    raise KeyError(f"Source config not found for name={name!r}")