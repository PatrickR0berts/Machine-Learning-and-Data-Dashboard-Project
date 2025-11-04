import pandas as pd
import wbgapi as wb

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