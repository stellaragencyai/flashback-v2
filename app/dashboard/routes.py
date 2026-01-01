from flask import Blueprint, render_template
from app.dashboard.data_hydrator_v1 import hydrate_dashboard_rows

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
def dashboard_view():
    rows = hydrate_dashboard_rows()
    return render_template(
        'dashboard.html',
        rows=rows,
        row_count=len(rows),
        schema_version=1
    )
