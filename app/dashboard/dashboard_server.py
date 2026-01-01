# app/dashboard/dashboard_server.py

from flask import Flask
from flask_socketio import SocketIO
from app.dashboard.routes import dashboard_bp

print("🔥 Initializing Dashboard Server...")

app = Flask(__name__)
app.config["SECRET_KEY"] = "flashback-dashboard"

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    logger=True,
    engineio_logger=True,
)

app.register_blueprint(dashboard_bp)

@app.route("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    print("🚀 Dashboard running on http://localhost:5000")
    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False,
    )
