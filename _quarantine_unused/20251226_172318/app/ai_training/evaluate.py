from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    roc_auc_score,
    classification_report,
)

from .data_loader import load_trade_dna, split_train_test
from .features import build_features
from .model_io import load_model

def evaluate_model():
    model = load_model()
    df = load_trade_dna()
    _, test_df = split_train_test(df)

    X_test, y_test = build_features(test_df)
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    print("Accuracy:", accuracy_score(y_test, y_pred))
    print("ROC AUC:", roc_auc_score(y_test, y_proba))
    print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))
    print(classification_report(y_test, y_pred))

if __name__ == "__main__":
    evaluate_model()
