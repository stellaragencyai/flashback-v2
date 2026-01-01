# app/cockpit/api.py
"""
Flashback Cockpit API — Phase C1.3
Sim-City metaphor wiring: City (global), Districts (subaccounts), Utilities (metrics), Events (alerts)
All endpoints are READ-ONLY and DRY-RUN SAFE.
"""

from flask import Blueprint, jsonify, request
from datetime import datetime

cockpit_api = Blueprint("cockpit_api", __name__, url_prefix="/api/cockpit")

# ---------------------------------------------------------------------------
# Mock adapters (to be replaced with real engine hooks)
# NOTE: Keep functions pure & non-blocking
# ---------------------------------------------------------------------------

def _now():
    return datetime.utcnow().isoformat() + "Z"

# Global City State -----------------------------------------------------------

def get_city_overview():
    return {
        "city": {
            "name": "Flashback City",
            "mode": "DRY_RUN",
            "uptime_sec": 86400,
            "health": "GREEN",
            "population": 10,  # districts
            "power_grid": "STABLE",
            "traffic": "FLOWING",
            "weather": "CALM",
            "timestamp": _now(),
        }
    }

# Districts (Subaccounts) -----------------------------------------------------

def get_districts():
    districts = []
    for i in range(1, 11):
        districts.append({
            "district_id": f"BOT_{i:02d}",
            "name": f"District {i}",
            "status": "ACTIVE",
            "economy": {
                "pnl_24h": round((i - 5) * 12.34, 2),
                "drawdown": round(abs(i - 6) * 0.7, 2),
                "exposure": round(i * 3.1, 2),
                "liquidity": "GOOD",
            },
            "utilities": {
                "cpu_load": round(10 + i * 2.1, 1),
                "gpu_load": round(20 + i * 1.7, 1),
                "memory_mb": 512 + i * 64,
                "latency_ms": 8 + i,
            },
            "ai": {
                "confidence": round(0.55 + i * 0.03, 2),
                "activity": "OBSERVING",
                "models": ["classifier_v1"],
            },
            "last_tick": _now(),
        })
    return {"districts": districts}

# Main Account (Discretionary) -------------------------------------------------

def get_main_account():
    return {
        "main_account": {
            "name": "Central Bank",
            "type": "DISCRETIONARY",
            "economy": {
                "pnl_24h": 1234.56,
                "drawdown": 0.0,
                "exposure": 0.0,
            },
            "utilities": {
                "cpu_load": 4.2,
                "memory_mb": 256,
                "latency_ms": 5,
            },
            "last_tick": _now(),
        }
    }

# Metrics & Utilities ----------------------------------------------------------

def get_city_metrics():
    return {
        "metrics": {
            "cpu": {"usage": 92.1, "cores": 20},
            "gpu": {"usage": 67.4, "memory_used_mb": 3890},
            "io": {"read_mb_s": 1240, "write_mb_s": 420},
            "jobs": {"queued": 0, "running": 19},
            "timestamp": _now(),
        }
    }

# Events (Alerts / Incidents) --------------------------------------------------

def get_events(limit=50):
    events = []
    for i in range(limit):
        events.append({
            "event_id": f"EVT_{i:04d}",
            "severity": "INFO" if i % 5 else "WARN",
            "source": "ENGINE",
            "message": "All systems nominal" if i % 5 else "Minor congestion detected",
            "timestamp": _now(),
        })
    return {"events": events}

# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@cockpit_api.route("/city", methods=["GET"])
def city():
    return jsonify(get_city_overview())

@cockpit_api.route("/districts", methods=["GET"])
def districts():
    return jsonify(get_districts())

@cockpit_api.route("/main-account", methods=["GET"])
def main_account():
    return jsonify(get_main_account())

@cockpit_api.route("/metrics", methods=["GET"])
def metrics():
    return jsonify(get_city_metrics())

@cockpit_api.route("/events", methods=["GET"])
def events():
    limit = int(request.args.get("limit", 50))
    return jsonify(get_events(limit))

# ---------------------------------------------------------------------------
# Integration note:
# Register blueprint in app factory:
#   app.register_blueprint(cockpit_api)
# ---------------------------------------------------------------------------
