import duckdb

# connection
conn = duckdb.connect('lendable.duckdb')

# i want to visualise and query the raw CSV directly
df = conn.execute("""
    SELECT *
    FROM read_csv_auto('/workspaces/snr_data_analyst_lendable/lendable/seeds/MI_Manager_Test-Raw_Data_cleaned.csv')
""").fetchdf()

# Preview
print(df.dtypes)
