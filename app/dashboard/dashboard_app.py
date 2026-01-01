from flask import Flask
from app.dashboard.routes import dashboard_bp

def create_app():
    app = Flask(
        __name__,
        template_folder='templates',
        static_folder='static'
    )
    app.register_blueprint(dashboard_bp)
    return app

app = create_app()
