from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score

from .data_loader import load_trade_dna, split_train_test
from .features import build_features
from .model_io import save_model

def train_model():
    df = load_trade_dna()
    train_df, test_df = split_train_test(df)

    X_train, y_train = build_features(train_df)
    X_test, y_test = build_features(test_df)

    # Baseline classifier
    model = RandomForestClassifier(n_estimators=200, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    print("=== TRAIN EVAL ===")
    print(classification_report(y_train, model.predict(X_train)))
    print("=== TEST EVAL ===")
    print(classification_report(y_test, y_pred))
    print("ACC:", accuracy_score(y_test, y_pred))

    model_path = save_model(model)
    print(f"Model saved to: {model_path}")

if __name__ == "__main__":
    train_model()
