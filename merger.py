import pandas as pd

# Read the CSV files
df1 = pd.read_csv("l4_login.csv")
df2 = pd.read_csv("l3_login.csv")

def merge_dataframes(df1, df2):
    """
    Concatenate two DataFrames, keeping all rows from both, and remove rows where the 'count' column is 0.
    
    Parameters:
    df1 (pd.DataFrame): First DataFrame.
    df2 (pd.DataFrame): Second DataFrame.

    Returns:
    pd.DataFrame: Concatenated DataFrame with rows where 'count' is not 0.
    """
    # Concatenate both DataFrames along rows (axis=0)
    merged_df = pd.concat([df1, df2], axis=0, ignore_index=True)
    
    # Remove rows where 'count' is 0
    merged_df = merged_df[merged_df['count'] != 0]
    
    return merged_df

# Merge the DataFrames and filter out rows where 'count' is 0
result = merge_dataframes(df1, df2)

# Print the resulting DataFrame
print(result)
