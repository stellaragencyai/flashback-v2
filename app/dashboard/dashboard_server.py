#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# app/dashboard/dashboard_server.py

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

# --- Windows-safe stdout/stderr ---
os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")


def _safe_console() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


_safe_console()

# ---------------- Imports with clear error messages ----------------
try:
    from flask import Flask, request, send_from_directory
except ImportError as e:
    print(f"[dashboard] ERROR: Flask is not installed or not importable: {e}")
    print("[dashboard] Fix: pip install flask")
    sys.exit(1)

try:
    from flask_socketio import SocketIO
except ImportError as e:
    print(f"[dashboard] ERROR: Flask-SocketIO is not installed or not importable: {e}")
    print("[dashboard] Fix: pip install flask-socketio")
    sys.exit(1)

try:
    from app.dashboard.routes import dashboard_bp
except ImportError as e:
    print(f"[dashboard] ERROR: Could not import dashboard routes blueprint: {e}")
    print("[dashboard] Expected: app/dashboard/routes.py defines dashboard_bp")
    sys.exit(1)


print("[dashboard] Initializing Dashboard Server...")

# Repo-root + static dir
_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]          # ...\flashback
STATIC_DIR = REPO_ROOT / "app" / "static"  # ...\flashback\app\static

# Keep Flask static enabled, but ALSO add a hard override route below.
app = Flask(
    __name__,
    static_folder=str(STATIC_DIR),
    static_url_path="/static",
)
app.config["SECRET_KEY"] = "flashback-dashboard"

# Dev-friendly: always reload templates + disable static cache
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


@app.after_request
def _no_cache_headers(resp: Any) -> Any:
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


# -------------------------------------------------------------------
# HARD STATIC OVERRIDE (fixes your 404)
# Forces /static/* to serve from C:\flashback\app\static no matter what.
# -------------------------------------------------------------------
@app.route("/static/<path:filename>")
def _static_force(filename: str):
    return send_from_directory(str(STATIC_DIR), filename)


socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    logger=True,
    engineio_logger=True,
)

# Register blueprint AFTER static override is defined
app.register_blueprint(dashboard_bp)


@app.route("/health")
def health() -> dict:
    return {"status": "ok"}


@app.route("/debug/ping", methods=["GET", "POST"])
def debug_ping() -> dict:
    return {
        "ok": True,
        "method": request.method,
        "args": dict(request.args),
    }


@app.route("/debug/routes")
def debug_routes() -> dict:
    # Useful when Flask decides to be "creative" about routing.
    rules = []
    for r in sorted(app.url_map.iter_rules(), key=lambda x: str(x)):
        rules.append({"rule": str(r), "endpoint": r.endpoint, "methods": sorted(list(r.methods))})
    return {"static_dir": str(STATIC_DIR), "routes": rules}


if __name__ == "__main__":
    print("[dashboard] Running on http://localhost:5000")
    print(f"[dashboard] REPO_ROOT={REPO_ROOT}")
    print(f"[dashboard] STATIC_DIR={STATIC_DIR} (url=/static)")
    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False,
    )
