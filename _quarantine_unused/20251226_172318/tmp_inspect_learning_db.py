import sqlite3
from app.ai.ai_learning_contract import LearningPaths

lp = LearningPaths.default()
print("DB PATH:", lp.learning_sqlite_path)

conn = sqlite3.connect(str(lp.learning_sqlite_path))
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
rows = cur.fetchall()

print("tables:")
for r in rows:
    print(" -", r[0])

conn.close()
