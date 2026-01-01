# Alert Severity & Escalation Rules v1.0

SEVERITY = {
    "ONLINE": "INFO",
    "OFFLINE": "CRITICAL",
    "DEGRADED": "ERROR",
    "RECOVERY": "INFO",
}

ESCALATION = {
    "restart_warn": 1,
    "restart_error": 3,
    "restart_critical": 5,
}

EMOJI = {
    "INFO": "??",
    "WARN": "??",
    "ERROR": "??",
    "CRITICAL": "??",
    "FLEET": "??",
    "RECOVERY": "??",
}
