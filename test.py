import sqlite3
import pandas as pd

conn = sqlite3.connect(r"C:\Users\n_lou\Desktop\New folder (3)\production.db")
df = pd.read_sql("SELECT * FROM quality_checks LIMIT 5;", conn)
print(df)
conn.close()