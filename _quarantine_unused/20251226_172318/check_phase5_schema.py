import sqlite3
from app.ai.ai_memory_contract import ContractPaths

cp = ContractPaths.default()
conn = sqlite3.connect(str(cp.memory_index_path))

cols = [r[1] for r in conn.execute("PRAGMA table_info(memory_entries)").fetchall()]
idx  = [r[1] for r in conn.execute("PRAGMA index_list(memory_entries)").fetchall()]

print("has_entry_id_col =", "entry_id" in cols)
print("has_trade_id_col =", "trade_id" in cols)
print("indexes =", idx)

row = conn.execute(
    "SELECT trade_id, entry_id, ts_ms "
    "FROM memory_entries "
    "WHERE trade_id=? "
    "ORDER BY ts_ms DESC LIMIT 1",
    ("flashback09:PIPE_POST_ENFORCE_002",),
).fetchone()

conn.close()

print("latest_row =", row)
