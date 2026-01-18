from pathlib import Path
import re

p = Path(r"C:\Flashback\app\ops\orchestrator_v1.py")
s = p.read_text(encoding="utf-8", errors="replace")

# Replace the "write state once and exit" tail with a live loop that refreshes state + restarts workers.
pat = re.compile(
    r"""
    \n[ \t]*out[ \t]*=[ \t]*\{.*?\n[ \t]*OUT\.write_text\(json\.dumps\(out, indent=2\), encoding="utf-8"\)\n
    [ \t]*\n[ \t]*print\(f"OK: orchestrator_v1.*?\n
    [ \t]*print\(f"OK: state=\{OUT\}"\)\n
    [ \t]*print\(f"OK: boot_record=\{BOOT\}"\)\n
    [ \t]*print\(f"OK: logs_dir=\{LOGDIR\}"\)\n
    [ \t]*return[ \t]+0\n
    """,
    re.DOTALL | re.VERBOSE
)

m = pat.search(s)
if not m:
    raise SystemExit("FATAL: Could not find orchestrator_v1 one-shot tail block to replace.")

replacement = r'''
    # -------------------------
    # LIVE RUNTIME LOOP (critical)
    # -------------------------
    # The old behavior wrote state once and exited, which is not orchestration.
    # We now stay alive, refresh state, and keep child supervisors healthy.

    tick_sec = _as_int(os.getenv("ORCH_TICK_SEC"), 5)
    if tick_sec < 1:
        tick_sec = 5

    restart_dead = os.getenv("ORCH_RESTART_DEAD", "true").strip().lower() in ("1","true","yes","y","on")

    def _read_hb_ms(label: str) -> int:
        try:
            fp = STATE / f"ws_switchboard_heartbeat_{label}.txt"
            if not fp.exists():
                return 0
            txt = fp.read_text(encoding="utf-8", errors="replace").strip()
            # file typically contains a ms timestamp or a short token; be defensive
            v = int("".join([c for c in txt if c.isdigit()]) or "0")
            return v
        except Exception:
            return 0

    print(f"OK: orchestrator_v1 mode={mode} started={len(started)} max_procs={max_procs}")
    print(f"OK: state={OUT}")
    print(f"OK: boot_record={BOOT}")
    print(f"OK: logs_dir={LOGDIR}")
    print(f"OK: tick_sec={tick_sec} restart_dead={restart_dead}")

    while True:
        # Update proc liveness + online status
        for label, meta in list(procs.items()):
            pid = int(meta.get("pid") or 0)
            alive = _pid_alive(pid) if pid else False
            meta["alive"] = bool(alive)

            entry = subaccounts_state.get(label) or {}
            entry["pid"] = pid or None
            entry["alive"] = bool(alive)
            entry["online"] = bool(alive)
            entry["status"] = "RUNNING" if alive else "DEAD"
            entry["reason"] = None if alive else "process not alive"
            entry["last_heartbeat_ms"] = _read_hb_ms(label)

            subaccounts_state[label] = entry

            # Restart if dead (best-effort, append logs)
            if (not alive) and restart_dead:
                try:
                    env = os.environ.copy()
                    env["ACCOUNT_LABEL"] = label
                    env["FLASHBACK_MODE"] = mode
                    env["PYTHONUTF8"] = "1"
                    env["PYTHONIOENCODING"] = "utf-8"

                    state_dir = ROOT / "state"
                    lab = (label or "").strip().lower()
                    if lab in ("", "main"):
                        env["POSITIONS_BUS_PATH"] = str(state_dir / "positions_bus.json")
                        env["ORDERBOOK_BUS_PATH"] = str(state_dir / "orderbook_bus.json")
                        env["TRADES_BUS_PATH"] = str(state_dir / "trades_bus.json")
                        env["PUBLIC_TRADES_PATH"] = str(state_dir / "public_trades.jsonl")
                        env["EXEC_BUS_PATH"] = str(state_dir / "ws_executions.jsonl")
                    else:
                        env["POSITIONS_BUS_PATH"] = str(state_dir / f"positions_bus_{lab}.json")
                        env["ORDERBOOK_BUS_PATH"] = str(state_dir / f"orderbook_bus_{lab}.json")
                        env["TRADES_BUS_PATH"] = str(state_dir / f"trades_bus_{lab}.json")
                        env["PUBLIC_TRADES_PATH"] = str(state_dir / f"public_trades_{lab}.jsonl")
                        env["EXEC_BUS_PATH"] = str(state_dir / f"ws_executions_{lab}.jsonl")

                    cmd = meta.get("cmd") or [sys.executable, "-m", "app.bots.supervisor_ai_stack"]

                    ts = _now_ms()
                    out_log = LOGDIR / f"{label}.stdout.log"
                    err_log = LOGDIR / f"{label}.stderr.log"

                    with out_log.open("ab") as fo, err_log.open("ab") as fe:
                        header = f"\n\n=== RESTART {label} ts_ms={ts} mode={mode} cmd={cmd} ===\n"
                        fo.write(header.encode("utf-8", errors="ignore"))
                        fe.write(header.encode("utf-8", errors="ignore"))
                        p = subprocess.Popen(
                            cmd,
                            cwd=str(ROOT),
                            env=env,
                            stdout=fo,
                            stderr=fe,
                            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
                        )

                    time.sleep(0.35)
                    meta["pid"] = int(p.pid)
                    meta["started_ts_ms"] = ts
                    meta["alive"] = _pid_alive(int(p.pid))

                    entry = subaccounts_state.get(label) or {}
                    entry["pid"] = int(p.pid)
                    entry["alive"] = bool(meta["alive"])
                    entry["online"] = bool(meta["alive"])
                    entry["status"] = "RUNNING" if meta["alive"] else "RESTARTED_NOT_CONFIRMED"
                    entry["reason"] = None if meta["alive"] else "restart not confirmed alive"
                    entry["started_ts_ms"] = ts
                    subaccounts_state[label] = entry

                except Exception as e:
                    entry = subaccounts_state.get(label) or {}
                    entry["status"] = "ERROR"
                    entry["reason"] = f"restart_error: {repr(e)}"
                    subaccounts_state[label] = entry

        out = {
            "ts_ms": _now_ms(),
            "mode": mode,
            "boot_record": str(BOOT),
            "manifest": str(MANIFEST),
            "only_labels": sorted(list(only_set)),
            "max_procs": max_procs,
            "started": started,
            "skipped": skipped,
            "procs": procs,
            "subaccounts": subaccounts_state,
        }

        try:
            OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
        except Exception:
            pass

        time.sleep(tick_sec)
'''

s2 = s[:m.start()] + replacement + s[m.end():]
p.write_text(s2, encoding="utf-8")
print("OK: patched runtime loop into", p)
