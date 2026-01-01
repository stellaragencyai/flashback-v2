import joblib
from pathlib import Path

MODEL_PATH = Path("state/models/trade_classifier.pkl")
_MODEL = None

def get_model():
    global _MODEL
    if _MODEL is None:
        _MODEL = joblib.load(MODEL_PATH)
    return _MODEL
