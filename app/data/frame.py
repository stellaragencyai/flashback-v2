import json
from pathlib import Path
import polars as pl

# ---------------- PATHS ----------------
FEATURE_PROGRESS = Path("state/features/progress.json")

# ---------------- PROGRESS ----------------
def load_last_feature_ts():
    if not FEATURE_PROGRESS.exists():
        return None
    try:
        return json.load(FEATURE_PROGRESS.open()).get("last_ts")
    except Exception:
        return None

def save_last_feature_ts(ts):
    FEATURE_PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    json.dump({"last_ts": ts}, FEATURE_PROGRESS.open("w"))

# ---------------- DATA IO ----------------
def read_market_data(path):
    return pl.read_parquet(
        path,
        memory_map=True
    )

def write_market_data(df, path):
    df.write_parquet(
        path,
        compression="zstd",
        statistics=True,
        append=Path(path).exists()
    )

# ---------------- FEATURE REGISTRY ----------------
FEATURE_REGISTRY = Path("state/features/registry.json")
ACTIVE_VERSION = Path("state/features/active_version.txt")

def get_active_feature_version():
    if not ACTIVE_VERSION.exists():
        return "v0"
    try:
        return ACTIVE_VERSION.read_text(encoding="utf-8-sig").strip()
    except Exception:
        return ACTIVE_VERSION.read_text().strip()


def load_feature_registry():
    if not FEATURE_REGISTRY.exists():
        return {}
    try:
        return json.load(FEATURE_REGISTRY.open())
    except Exception:
        return {}

def save_feature_registry(reg):
    FEATURE_REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    json.dump(reg, FEATURE_REGISTRY.open("w"), indent=2)

def register_feature_version(version, schema):
    reg = load_feature_registry()
    if version in reg:
        if reg[version]["schema"] != schema:
            raise RuntimeError(
                f"Feature schema mismatch for version {version}"
            )
        return
    reg[version] = {
        "schema": schema
    }
    save_feature_registry(reg)

