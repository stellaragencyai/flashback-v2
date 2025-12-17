import joblib
from .config import MODEL_OUTPUT_DIR

def save_model(model, name: str = "trade_classifier.pkl"):
    path = MODEL_OUTPUT_DIR / name
    joblib.dump(model, path)
    return path

def load_model(name: str = "trade_classifier.pkl"):
    path = MODEL_OUTPUT_DIR / name
    return joblib.load(path)
