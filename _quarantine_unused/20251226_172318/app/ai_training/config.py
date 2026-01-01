from pathlib import Path

ROOT = Path(__file__).parent.parent  # project root
TELEMETRY_DIR = ROOT / "state"
TRADE_DNA_FILE = TELEMETRY_DIR / "trade_dna.jsonl"
MODEL_OUTPUT_DIR = ROOT / "models"

# Training parameters
TEST_SPLIT_RATIO = 0.2
RANDOM_SEED = 42

# Make sure model output dir exists
MODEL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
