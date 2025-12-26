from __future__ import annotations

from pathlib import Path

TARGET = Path(r"app\bots\signal_engine.py")

def main() -> int:
    if not TARGET.exists():
        print(f"FAIL: missing {TARGET}")
        return 2

    lines = TARGET.read_text(encoding="utf-8", errors="ignore").splitlines()

    # We will inject: m_debug["regime"] = regime_ind
    # right after existing m_debug construction line if present.
    # We saw you already have code that builds m_debug with {"setup":..., "regime":...} in one spot,
    # but your emitted debug doesn't include it, so we force-set it.

    out = []
    injected = 0

    for i, l in enumerate(lines):
        out.append(l)

        # Look for the specific m_debug creation line you already have (from your grep):
        # m_debug: Dict[str, Any] = {"setup": setup, "regime": regime_ind, "signal_origin": "strategy"}
        if "m_debug" in l and "signal_origin" in l and "regime_ind" in l:
            # It already includes regime_ind, so no need here.
            continue

        # More robust: after we compute regime_ind, ensure it gets attached to whatever debug dict exists.
        # Find lines like: regime_ind = compute_regime_indicators(candles)
        if "regime_ind = compute_regime_indicators" in l:
            out.append("            # PATCH: ensure regime indicators are emitted into observed.debug")
            out.append("            try:")
            out.append("                # later debug dicts should include this under key 'regime'")
            out.append("                _fb_regime_ind = regime_ind")
            out.append("            except Exception:")
            out.append("                _fb_regime_ind = None")
            injected += 1

        # Find where fb_debug is created and add fb_debug['regime']=regime_ind if missing
        if "fb_debug" in l and "=" in l and "{" in l and "Dict" in l:
            # too risky to match types; skip
            pass

        # Find where we assign fb_debug fields and ensure regime is set
        if "fb_debug[\"regime\"]" in l or "fb_debug['regime']" in l:
            # already present somewhere
            continue

    # Second pass: if there's a dict called fb_debug being used, force-attach regime before any emit call
    text = "\n".join(out)

    if "regime_tags=regime_tags" in text and "_fb_regime_ind" in text and "fb_debug" in text:
        # Attempt to inject right before first occurrence of "regime_tags=regime_tags"
        marker = "regime_tags=regime_tags,"
        if "fb_debug" in text:
            text = text.replace(
                marker,
                "                # PATCH: attach regime indicators into debug payload\n"
                "                try:\n"
                "                    if isinstance(fb_debug, dict) and _fb_regime_ind is not None:\n"
                "                        fb_debug[\"regime\"] = _fb_regime_ind\n"
                "                except Exception:\n"
                "                    pass\n"
                f"{marker}",
                1
            )
            injected += 1

    TARGET.write_text(text, encoding="utf-8", newline="\n")
    print(f"OK: patched signal_engine.py (injected={injected})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
