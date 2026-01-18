from pathlib import Path

p = Path(r"app/dashboard/routes.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "/api/action" in s:
    print("PATCH_NOOP: action endpoint already exists")
    raise SystemExit(0)

insert = """

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
"""

s = s.rstrip() + insert
p.write_text(s, encoding="utf-8")
print("OK: /api/action endpoint added (safe no-op)")
