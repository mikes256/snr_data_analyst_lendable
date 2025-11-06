import pandas as pd

df = pd.read_csv('/workspaces/snr_data_analyst_lendable/lendable/raw_csv_data/MI_Manager_Test-Raw_Data-MI_Manager_Test-Raw_Data.csv', dtype={'ID': str}) # ensure 'ID' is read as string to allow duckdb to infer correct type
print(df.dtypes)
#df.to_csv('/workspaces/snr_data_analyst_lendable/lendable/seeds/MI_Manager_Test-Raw_Data_cleaned.csv', index=False)
print("Cleaned CSV saved as 'MI_Manager_Test-Raw_Data_cleaned.csv'")
