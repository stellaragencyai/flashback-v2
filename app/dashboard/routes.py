from flask import Blueprint, render_template, jsonify, request
from app.dashboard.data_hydrator_v1 import hydrate_dashboard_rows, hydrate_dashboard_meta

dashboard_bp = Blueprint("dashboard", __name__)

@dashboard_bp.route("/")
def dashboard_view():
    window = (request.args.get("window") or "all").strip().lower()
    if window not in ("7d", "30d", "1y", "all"):
        window = "all"

    rows = hydrate_dashboard_rows(window=window)
    meta = hydrate_dashboard_meta(window=window)

    return render_template(
        "dashboard.html",
        rows=rows,
        meta=meta,
        row_count=len(rows),
        schema_version=1,
        window=window
    )

@dashboard_bp.route("/api/subaccounts")
def api_subaccounts():
    window = (request.args.get("window") or "all").strip().lower()
    if window not in ("7d", "30d", "1y", "all"):
        window = "all"
    return jsonify(hydrate_dashboard_rows(window=window))

@dashboard_bp.route("/api/dashboard_meta")
def api_dashboard_meta():
    window = (request.args.get("window") or "all").strip().lower()
    if window not in ("7d", "30d", "1y", "all"):
        window = "all"
    return jsonify(hydrate_dashboard_meta(window=window))

@dashboard_bp.route("/api/action", methods=["POST"])
def api_action():
    data = request.get_json(force=True, silent=True) or {}
    acct = data.get("account")
    action = data.get("action")

    if not acct or action not in ("start","stop","restart","ping"):
        return jsonify({"ok": False, "error": "invalid request"}), 400

    # SAFETY: no execution yet
    print(f"[dashboard] ACTION_REQUEST account={acct} action={action}")

    return jsonify({
        "ok": True,
        "account": acct,
        "action": action,
        "note": "Accepted (no-op, safety mode)"
    })
