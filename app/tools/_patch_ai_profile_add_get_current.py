from pathlib import Path
import re

p = Path(r"app\core\ai_profile.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if re.search(r"^\s*def\s+get_current_ai_profile\s*\(", s, flags=re.M):
    print("OK: get_current_ai_profile already exists (no patch).")
else:
    shim = r"""

# ---------------------------------------------------------------------------
# COMPAT SHIM (2026-01-01): expected by ai_action_router and legacy callers
# ---------------------------------------------------------------------------

def get_current_ai_profile(account_label: str = "main") -> dict:
    \"\"\"Return the active AI profile for a given account_label.

    This is a backward-compatible API. Newer code may expose a differently named
    function. We try common candidates and fall back safely.
    \"\"\"
    # Try common modern function names first (if your codebase renamed it).
    candidates = [
        "get_active_ai_profile",
        "get_ai_profile",
        "load_ai_profile",
        "resolve_ai_profile",
        "get_profile",
    ]

    g = globals()
    for name in candidates:
        fn = g.get(name)
        if callable(fn):
            try:
                out = fn(account_label)  # preferred signature
            except TypeError:
                try:
                    out = fn()  # alternate signature
                except Exception:
                    continue
            except Exception:
                continue

            if isinstance(out, dict):
                out.setdefault("account_label", account_label)
                out.setdefault("profile_name", out.get("name") or out.get("profile") or "default")
                out.setdefault("profile_version", out.get("version") or "unknown")
                return out

    # Safe fallback: do not crash the stack if profiles aren't wired yet.
    return {
        "account_label": account_label,
        "profile_name": "default",
        "profile_version": "unknown",
        "enabled": True,
        "notes": "compat fallback (no profile resolver found)",
    }
"""
    # Ensure file ends with newline before append
    if not s.endswith("\n"):
        s += "\n"
    s += shim
    p.write_text(s, encoding="utf-8")
    print("OK: appended compat shim get_current_ai_profile() to ai_profile.py")
