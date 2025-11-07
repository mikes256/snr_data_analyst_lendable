import sqlite3
import pandas as pd
from pathlib import Path


data = Path() / '/workspaces/snr_data_analyst_lendable/MI Manager Test - Raw Data - MI Manager Test - Raw Data.csv'
df = pd.read_csv(data)
conn = sqlite3.connect('conversations.db')
df.to_sql('conversations', conn, index=False, if_exists='replace')
conn.close()
