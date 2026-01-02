from __future__ import annotations

from pathlib import Path
import re

TARGET = Path(r"app\bots\supervisor_ai_stack.py")

def main():
    if not TARGET.exists():
        raise SystemExit(f"STOP: missing {TARGET}")

    src = TARGET.read_text(encoding="utf-8", errors="ignore")

    # 1) Replace the fragile import gate with a file-path subprocess validator.
    # We anchor on the specific import line shown in your traceback.
    if "from app.tools.validate_config import main as validate_config_main" not in src:
        print("WARN: anchor import line not found; patch may already be applied or file differs.")
    else:
        # Replace the whole try/except block that contains that import with a robust runner.
        # We locate the surrounding try/except by grabbing a chunk around the import.
        # This is intentionally conservative: we only patch if we can find the block.
        pattern = r"""
(?sx)
\s*try:\s*
\s*from\ app\.tools\.validate_config\ import\ main\ as\ validate_config_main\s*#\ type:\ ignore\s*
\s*except\ Exception\ as\ e:\s*
"""
        m = re.search(pattern, src)
        if not m:
            raise SystemExit("STOP: could not locate validate_config import try/except block")

        # Insert a helper runner just before the try:
        runner = r'''
def _run_validate_config_subprocess() -> tuple[bool, str]:
    """
    Runs config validator by script path (works even if app.tools is not a package).
    Returns (ok, message).
    """
    try:
        import subprocess, sys
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        script = root / "app" / "tools" / "validate_config.py"
        if not script.exists():
            return (False, f"Config validator missing at {script}")

        p = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(root),
            capture_output=True,
            text=True,
        )
        out = (p.stdout or "")[-2000:]
        err = (p.stderr or "")[-2000:]
        blob = (out + ("\n" if out and err else "") + err).strip()
        if p.returncode == 0:
            return (True, blob or "PASS")
        return (False, blob or f"validator rc={p.returncode}")
    except Exception as e:
        return (False, f"validator exception: {e!r}")
'''

        # Avoid double-inserting runner
        if "_run_validate_config_subprocess" not in src:
            # put runner near the top, after imports (after the first blank line following imports)
            lines = src.splitlines(True)
            insert_at = 0
            for i in range(min(200, len(lines))):
                if lines[i].strip() == "":
                    insert_at = i + 1
                    break
            lines = lines[:insert_at] + [runner + "\n"] + lines[insert_at:]
            src = "".join(lines)

        # Now replace the logic inside _hard_gate_validate_config to use the subprocess runner
        # We patch by replacing the import attempt line with a call to runner and return.
        src = src.replace(
            "from app.tools.validate_config import main as validate_config_main  # type: ignore",
            "# NOTE: app.tools may not be an importable package; run validator by file path\n        ok, detail = _run_validate_config_subprocess()\n        if not ok:\n            msg = f\"STOP Config validation failed: {detail}\"\n            log.error(msg)\n            try:\n                send_tg(msg)\n            except Exception:\n                pass\n            return False\n        return True\n\n        # legacy import path removed:",
        )

        # Also remove emoji in the known error message to avoid cp1252 crashes.
        src = src.replace("🛑", "STOP")

    TARGET.write_text(src, encoding="utf-8")
    print("OK: patched supervisor_ai_stack.py (validator import -> subprocess; emoji-safe).")

if __name__ == "__main__":
    main()
