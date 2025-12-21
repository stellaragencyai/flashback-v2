import sqlite3
from app.ai.ai_learning_contract import LearningPaths

lp = LearningPaths.default()
conn = sqlite3.connect(str(lp.learning_sqlite_path))
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
print("tables:")
for r in cur.fetchall():
    print(" -", r[0])

cur.execute("SELECT COUNT(*) FROM memory_stats_v1")
print("stats_rows:", cur.fetchone()[0])

cur.execute("SELECT MAX(n) FROM memory_stats_v1")
print("max_n:", cur.fetchone()[0])

cur.execute("""
SELECT policy_hash, symbol, timeframe, setup_type, n
FROM memory_stats_v1
ORDER BY n DESC
""")
print("ALL_ROWS:")
for r in cur.fetchall():
    print(r)

conn.close()
