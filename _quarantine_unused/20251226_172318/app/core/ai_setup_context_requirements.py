from typing import Dict, Any, Optional


# NOTE:
# The system has multiple producers of setup_context (executor, pilot, tools).
# Some provide `features` top-level, others embed feature hints under payload.features.
# This validator must accept BOTH to avoid rejecting valid setup_context rows.


REQUIRED_SETUP_KEYS = (
    "trade_id",
    "symbol",
    "account_label",
    "strategy",
    "setup_type",
    "timeframe",
)

# We still want these conceptually required, but they may live in different places.
REQUIRED_FEATURE_KEYS = (
    "memory_fingerprint",
)


def _safe_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""


def _extract_features(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accept features from:
      1) ctx["features"] (canonical executor build_setup_context style)
      2) ctx["payload"]["features"] (pilot-style input hints)
    Prefer ctx["features"] if present.
    """
    feats = ctx.get("features")
    if isinstance(feats, dict):
        return feats

    payload = ctx.get("payload")
    if isinstance(payload, dict):
        pf = payload.get("features")
        if isinstance(pf, dict):
            return pf

    return {}


def _extract_memory_fingerprint(ctx: Dict[str, Any]) -> str:
    """
    Accept memory_fingerprint from:
      1) ctx["memory_fingerprint"] (legacy / older rows)
      2) ctx["features"]["memory_fingerprint"] (canonical)
      3) ctx["payload"]["features"]["memory_fingerprint"] (pilot hints)
    """
    mf = _safe_str(ctx.get("memory_fingerprint") or "")
    if mf:
        return mf

    feats = _extract_features(ctx)
    mf2 = _safe_str(feats.get("memory_fingerprint") or "")
    if mf2:
        return mf2

    return ""


def has_valid_setup_context(ctx: Dict[str, Any]) -> bool:
    if not isinstance(ctx, dict):
        return False

    for k in REQUIRED_SETUP_KEYS:
        if k not in ctx:
            return False
        if _safe_str(ctx.get(k)) == "":
            return False

    feats = _extract_features(ctx)
    if not isinstance(feats, dict) or not feats:
        # features must exist somewhere even if minimal
        return False

    # memory_fingerprint must exist (can be empty during early cold-start),
    # but if it's empty you should treat it as "valid shape, weak signal".
    # Here we accept empty as valid *presence* only if the key exists.
    mf_top = "memory_fingerprint" in ctx
    mf_feats = "memory_fingerprint" in feats
    if not (mf_top or mf_feats):
        return False

    # Optional: if you want to enforce non-empty later, flip this on.
    # mf = _extract_memory_fingerprint(ctx)
    # if not mf:
    #     return False

    return True
