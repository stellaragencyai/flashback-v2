import pandas as pd
from sklearn.preprocessing import OneHotEncoder

def build_features(df: pd.DataFrame) -> (pd.DataFrame, pd.Series):
    """
    Build feature matrix X and label vector y.
    """

    # Label: profitable if pnl > 0
    df["label_win"] = (df["execution_outcome.pnl"] > 0).astype(int)

    # Example numeric features
    numeric_cols = [
        "signal_features.some_numeric_indicator",
        "regime_context.computed_adx",
        "regime_context.computed_atr_pct",
        "regime_context.computed_vol_z",
    ]

    # You might have other numeric fields in your signal_features
    numeric_present = [c for c in numeric_cols if c in df.columns]

    X_num = df[numeric_present].fillna(0.0)

    # One-hot encode tags
    tags_expanded = df["tags"].explode().to_frame()
    ohe = OneHotEncoder(handle_unknown="ignore", sparse=False)
    ohe_tags = pd.DataFrame(ohe.fit_transform(tags_expanded), index=tags_expanded.index)

    # Re-aggregate one-hot encoded tags back into row shape
    X_tags = (
        tags_expanded.join(ohe_tags)
        .groupby(level=0)
        .sum()
    )

    # Combine numeric + tag features
    X = pd.concat([X_num, X_tags], axis=1).fillna(0.0)
    y = df["label_win"]

    return X, y
