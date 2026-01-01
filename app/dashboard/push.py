from app.dashboard.hydrate import hydrate_dashboard_rows
from app.dashboard.dashboard_server import push_state

def broadcast_dashboard():
    rows = hydrate_dashboard_rows()
    push_state(rows)
