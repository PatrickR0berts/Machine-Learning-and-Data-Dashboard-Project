import pandas as pd
import wbgapi as wb
import numpy as np
import time
from pathlib import Path
from contextlib import redirect_stdout

#venv\Scripts\activate 



def CleanData(data, row_threshold=0.5, col_threshold=0.14):
    """
    Clean the data by removing rows with a high proportion of NaN values.

    Parameters:
    data (pd.DataFrame): The DataFrame to be cleaned.
    na_threshold (float): The threshold proportion of NaN values to determine row removal.

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """

    meta_cols = data.columns[:4]   # first 4 = metadata
    year_cols = data.columns[4:]   # remaining = years

    # Row mask: keep rows with <= na_threshold missing across year columns
    row_mask = data[year_cols].isnull().mean(axis=1) <= row_threshold # Create row mask based on threshold.
    print(f"Row mask created with threshold {row_threshold:.2%}") # Display row
    reduced_row_data = data.loc[row_mask].copy() # Apply row mask to data.
    print(f"After row cleaning: {reduced_row_data.shape[0]} rows kept out of {data.shape[0]}") # Display row cleaning result.
    
    # Column mask: keep columns with <= col_threshold missing across reduced data
    missing_per_col = reduced_row_data[year_cols].isnull().mean() # Calculate missing values per column.
    cols_to_keep = missing_per_col[missing_per_col <= col_threshold].index # Identify columns to keep based on threshold.
    print(f"Columns to keep based on threshold {col_threshold:.2%}: {len(cols_to_keep)} out of {len(year_cols)}") # Display column cleaning result.
    
    
    cleaned_data = reduced_row_data.loc[:, meta_cols.to_list() + cols_to_keep.to_list()].copy() # Combine metadata and kept year columns.

    return cleaned_data 
    

def ProcessData(data):
    
    """Process and clean the data for analysis.
    
    Parameters:
    data (pd.DataFrame, optional): The DataFrame to be processed.

    Returns:
    pd.DataFrame: The processed and cleaned DataFrame.  
    
"""

    if data is None:
        print("No data provided, exiting processing.")
        return None
    # Clean data: remove columns with all NaN values.
    # need to drop columns with all NaN values later during data processing.
    # need to also preprocess data to be standardised for ingestion into a K-means clustering model later.

   
    print("\n\n--- Data Cleaning Summary ---:\n")
    global_missing = data.isnull().mean().mean()
    print(f"Global missing value ratio: {global_missing:.2%}")
    
    missing_per_column = data.isnull().mean().sort_values(ascending=False)
    print(missing_per_column.head(10))  # Display top 10 columns with highest missing value ratios.
    missing_by_row = data.isnull().mean(axis=1)
    print(missing_by_row.describe())  # Display statistics of missing values by row.
    print("\n\n\n")

    data_cleaned = CleanData(data=data, row_threshold=0.5, col_threshold=0.14)  # Clean data with specified thresholds.

    data_cleaned_and_processed = data_cleaned # Further processing can be added here.


    #data_cleaned = data.dropna(axis=0, how=thresh=)
    #data_cleaned.to_csv('EU_and_UK_economic_data_cleaned.csv')  #
    print("Cleaned Data saved to CSV!")  # Display the cleaned data.
    #print(data_cleaned.head())  # Display the cleaned data.
    #print(data_cleaned.info())  # Display the cleaned data info.
    time.sleep(2)  # Pause for 2 seconds before ending the script.
    print("Script completed.")
    return data_cleaned_and_processed

def DataQualitySummary_NA_Check(data):
    print("\n\n--- Data Cleaning Summary ---:\n")
    global_missing = data.isnull().mean().mean()
    print(f"Global missing value ratio: {global_missing:.2%}")
    
    missing_per_column = data.isnull().mean().sort_values(ascending=False)
    print(missing_per_column.head(10))  # Display top 10 columns with highest missing value ratios.
    missing_by_row = data.isnull().mean(axis=1)
    print(missing_by_row.describe())  # Display statistics of missing values by row.
    print("\n\n\n")

# Read data from CSV file.
data = pd.read_csv(
    Path("data") / "EU_and_UK_economic_data.csv",
    na_values=["",".."," "],
    keep_default_na=False,
    index_col=0
)


DataQualitySummary_NA_Check(data) # Display the initial data quality summary.
with open("data_quality_summary.txt", "w") as f:
    with redirect_stdout(f):
        print("\n\n--- Initial Data Cleaning Summary ---:\n")
        DataQualitySummary_NA_Check(data)  # Save the data quality summary to a text file.
       
print("Data Quality Summary saved to data_quality_summary.txt")

# Process and clean the data.
processed_data = ProcessData(data)

processed_data.to_csv('data\EU_and_UK_economic_data_cleaned_and_processed.csv')  # Save cleaned data to CSV file.
print("Processed Data saved to CSV!")  # Confirm data saved.


DataQualitySummary_NA_Check(processed_data) # Display the processed data quality summary.
with open("data_quality_summary.txt", "a") as f:
    with redirect_stdout(f):
        print("\n\n--- Processed Data Cleaning Summary ---:\n")
        DataQualitySummary_NA_Check(processed_data)  # Save the processed data quality summary to a text file.

print("Processed Data Quality Summary appended to data_quality_summary.txt")

print("Data Ingestion and Cleaning Completed.")
print("Ready for further analysis or modeling.")