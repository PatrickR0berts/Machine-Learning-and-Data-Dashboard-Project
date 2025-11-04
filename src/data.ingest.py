import pandas as pd
import wbgapi as wb
import numpy as np
import time
from pathlib import Path

#venv\Scripts\activate 
eu_members_list =list(wb.region.members('EUU'))
countries_to_fetch = eu_members_list + ['GBR']


def fetch_wb_data(indicator, countries=countries_to_fetch, start_year=2000, end_year=2024):
    """
    Fetch data from the World Bank API for a given indicator and countries.

    Parameters:
    indicator (str): The World Bank indicator code.
    countries (list or str): List of country codes or 'all' for all countries.
    start_year (int): The starting year for data retrieval.
    end_year (int): The ending year for data retrieval.

    Returns:
    pd.DataFrame: A DataFrame containing the fetched data.
    """
    # Fetch data using wbgapi
    data = wb.data.DataFrame(indicator, economy=countries, time=range(start_year, end_year + 1))
    
    # Reset index to have a flat DataFrame
    data.reset_index(inplace=True)
    
    return data

def GetData():
    print(wb.source.info())  # Display available data sources - WDI = 2.

    print(wb.series.info('SP.DYN.LE00.IN')) #Display info about life expectancy indicator.
    # Display available regions. EUU = European Union. GBR = United Kingdom.



    #Get life expectancy data for the UK from 2000 to 2024.
    # data = wb.data.DataFrame(['SP.DYN.LE00.IN','NY.GDP.PCAP.CD'], economy=countries_to_fetch, time=range(2000, 2024)) 
    data = wb.data.DataFrame(['all'], economy=countries_to_fetch, skipBlanks=True, time=range(2000, 2024))
    print("Data fetched from WB API, processing...")
    print("Fetched and processed Data!")  # Display the fetched data.
    data.to_csv('EU_and_UK_economic_data.csv')  # Save data to CSV file. Need to check if index=False works as intended.
    print("Data saved to CSV!")  # Confirm data saved.
    print(data.head())  # Display the fetched data.
    print(data.info())  # Display the fetched data info.


def CleanData(data, na_threshold=0.5):
    """
    Clean the data by removing rows with a high proportion of NaN values.

    Parameters:
    data (pd.DataFrame): The DataFrame to be cleaned.
    na_threshold (float): The threshold proportion of NaN values to determine row removal.

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """
    year_cols = [c for c in data.columns if any(ch.isdigit() for ch in c)] # Identify year columns.
    mask = data[year_cols].isnull().mean(axis=1) <= 0.5 # Create mask for rows with acceptable NaN proportion.
    cleaned_data = data.loc[mask].copy() # Apply mask to filter rows.

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

    data_cleaned = CleanData(data=data, na_threshold=0.5)

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
    Path("data") / "EU_and_UK_economic_data_cleaned_and_processed.csv",
    na_values=["",".."," "],
    keep_default_na=False,
    index_col=0
)


DataQualitySummary_NA_Check(data)

#processed_data = ProcessData(data)
#processed_data.to_csv('data\EU_and_UK_economic_data_cleaned_and_processed.csv')  # Save cleaned data to CSV file.
#print(processed_data.head())
print("Data Ingestion and Cleaning Completed.")
print("Ready for further analysis or modeling.")