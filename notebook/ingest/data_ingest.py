import pandas as pd

def load_data(file_path):
    """
    Load data from a CSV file into a pandas DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        print(f"Loaded {len(df)} records from {file_path}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def main():
    # Sample usage
    file_path = "sample_data.csv"
    df = load_data(file_path)
    if df is not None:
        print(df.head())

if __name__ == "__main__":
    main()