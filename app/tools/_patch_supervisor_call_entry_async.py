from pathlib import Path
import re

p = Path(r"app\bots\supervisor_ai_stack.py")
s = p.read_text(encoding="utf-8")

pat = r'def _call_entry\(log, module, bot_name: str\) -> None:\n([\s\S]*?)\n\s*raise AttributeError\(f"\{module\.__name__\} has no callable main/loop/run"\)'

m = re.search(pat, s)
if not m:
    raise SystemExit("Could not find _call_entry block")

new = '''def _call_entry(log, module, bot_name: str) -> None:
    import inspect
    import asyncio

    for fn_name in ("main", "loop", "run"):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            log.info("%s entry: %s.%s()", bot_name, module.__name__, fn_name)
            res = fn()
            # If the entrypoint is async, run it properly.
            if inspect.iscoroutine(res):
                asyncio.run(res)
            return
    raise AttributeError(f"{module.__name__} has no callable main/loop/run")'''

s2 = s[:m.start()] + new + s[m.end():]
p.write_text(s2, encoding="utf-8")
print("OK: patched _call_entry to support async main()")
