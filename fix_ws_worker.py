from pathlib import Path

p = Path(r"app\bots\supervisor_ai_stack.py")
t = p.read_text(encoding="utf-8")

t = t.replace(
    'WorkerSpec("main", ws, _run_ws_switchboard)',
    'WorkerSpec("ws_switchboard", ws, _run_ws_switchboard)'
)

t = t.replace(
    'specs["main"].enabled',
    'specs["ws_switchboard"].enabled'
)

t = t.replace(
    '"main": WorkerSpec("ws_switchboard", ws, _run_ws_switchboard)',
    '"ws_switchboard": WorkerSpec("ws_switchboard", ws, _run_ws_switchboard)'
)

p.write_text(t, encoding="utf-8")
print("OK: supervisor worker name fully restored to ws_switchboard")
