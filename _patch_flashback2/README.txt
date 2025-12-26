Flashback 2.0 Patchset (first 7 enhancements - v0.1)

Files:
- spine_api.py (NEW): drop into app/core/spine_api.py
- ai_decision_logger.py (UPDATED): uses spine_api paths + now_ms
- ai_decision_enforcer.py (UPDATED): uses spine_api paths + fast tail reader
- ai_decision_outcome_linker.py (UPDATED): uses spine_api paths + fast tail reader
- ws_health_check.py (UPDATED): uses spine_api paths + consistent staleness calc
- ai_events_spine.py (UPDATED): consumes spine_api path constants (non-breaking)

All changes are fail-soft and backwards compatible (imports guarded).
