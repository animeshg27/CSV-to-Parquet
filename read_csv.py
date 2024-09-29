import pandas as pd

def load_csv(file_path):
    try:
        data = pd.read_csv(file_path)
        print("CSV file loaded successfully")
        return data
    except:
        print("Error file in loading data")
        return None

def clean_data(df):
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    return df

def convert_to_parquet(df, parquet_file_path):
    try:
        df.to_parquet(parquet_file_path, engine="pyarrow", compression="snappy")
        print(f"Data successfully writte in parquest: {parquet_file_path}")
    except Exception as e:
        print(f"Error converting data to parquet: {e}")


file_path = "employees.csv"
parquet_file_path = "output.parquet"
df = load_csv(file_path)
df_cleaned = clean_data(df)

if df is not None:
    print(df_cleaned.isnull().sum())

convert_to_parquet(df_cleaned, parquet_file_path)
