import sqlite3
from app.ai.ai_memory_contract import ContractPaths

p = ContractPaths.default().memory_index_path
conn = sqlite3.connect(str(p))
cur = conn.cursor()

cur.execute("""
SELECT
  policy_hash,
  symbol,
  timeframe,
  COALESCE(setup_type, '') AS setup_type,
  COUNT(*) AS n
FROM memory_entries
GROUP BY policy_hash, symbol, timeframe, COALESCE(setup_type, '')
ORDER BY n DESC
LIMIT 20
""")

print("TOP20_BUCKETS")
for row in cur.fetchall():
    print(row)

conn.close()
