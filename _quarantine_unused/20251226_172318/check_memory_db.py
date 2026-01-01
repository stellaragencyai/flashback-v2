import os
import sqlite3
from app.ai.ai_memory_contract import ContractPaths

p = ContractPaths.default().memory_index_path

print("db_path =", p)
print("exists =", os.path.exists(p))
print("size_bytes =", os.path.getsize(p) if os.path.exists(p) else None)

with open(p, "rb") as f:
    print("header =", f.read(16))

conn = sqlite3.connect(str(p))
cur = conn.execute(
    "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
)
rows = cur.fetchall()

print("tables =", len(rows))
for name, sql in rows:
    print("-", name)
    print(sql)

conn.close()
