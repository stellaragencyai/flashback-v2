import sqlite3
from app.ai.ai_memory_contract import ContractPaths

p = ContractPaths.default().memory_index_path
conn = sqlite3.connect(str(p))
cur = conn.cursor()

sql = """
SELECT n, COUNT(*)
FROM (
    SELECT
        (policy_hash || '|' || symbol || '|' || timeframe || '|' || COALESCE(setup_type, '')) AS k,
        COUNT(*) AS n
    FROM memory_entries
    GROUP BY k
)
GROUP BY n
ORDER BY n;
"""

cur.execute(sql)
print(cur.fetchall())

conn.close()
