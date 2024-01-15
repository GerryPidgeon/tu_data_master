import pandas as pd
import numpy as np
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from functions.shared_functions import clean_location_names

def process_deliverect_shared_data(df):
    # Convert 'OrderID' to string and prepend each ID with a '#' symbol
    # 'astype(str)' is used to ensure 'OrderID' is treated as a string for concatenation
    df['OrderID'] = '#' + df['OrderID'].astype(str)

    # Convert and standardize the 'OrderPlacedDateTime' to UTC, then to 'Europe/Berlin' timezone
    # This ensures consistency in time representation across the dataset
    # Additionally, separate the date and time components for more granular analysis
    df['OrderPlacedDateTime'] = pd.to_datetime(df['OrderPlacedDateTime']).dt.tz_localize('UTC')
    df['OrderPlacedDateTime'] = df['OrderPlacedDateTime'].dt.tz_convert('Europe/Berlin')
    df['OrderPlacedDate'] = df['OrderPlacedDateTime'].dt.date
    df['OrderPlacedTime'] = df['OrderPlacedDateTime'].dt.time

    # Clean and standardize 'Brand' names
    # First, extract the primary brand name if multiple brands are listed (separated by a comma)
    df['Brand'] = df['Brand'].apply(lambda x: str(x).split(',')[0] if isinstance(x, str) and ',' in x else x)

    # Handle missing Brand values based on 'Location' contents
    # Assign 'Birria & the Beast' or 'Birdie Birdie' to null Brand values depending on the presence of 'beast' in 'Location'
    df['Brand'] = np.where(df['Brand'].isnull(), np.where(df['Location'].str.contains('beast', case=False), 'Birria & the Beast', 'Birdie Birdie'), df['Brand'])

    # Further standardize 'Brand' names by replacing any occurrence of 'beast' with 'Birria' and others with 'Birdie'
    df['Brand'] = np.where(df['Brand'].str.contains('beast', case=False), 'Birria', 'Birdie')

    # Clean and standardize location names using a shared function
    df = clean_location_names(df)

    # Combine 'Location' and 'Brand' into a new column 'LocWithBrand' for better identification
    df['LocWithBrand'] = df['Location'] + ' - ' + df['Brand'].str.split(n=1).str[0]

    # Clean and standardize other columns for consistency
    df['Channel'] = df['Channel'].str.replace('TakeAway Com', 'Lieferando')
    df['OrderStatus'] = df['OrderStatus'].str.replace('_', ' ').str.title()
    df = df[df['OrderStatus'] != 'Duplicate']

    # Construct a 'PrimaryKey' for each row by concatenating 'OrderID', 'Location', and 'OrderPlacedDate'
    # This unique identifier helps in distinguishing each order entry unambiguously
    df['PrimaryKey'] = df['OrderID'] + ' - ' + df['Location'] + ' - ' + df['OrderPlacedDate'].astype(str)
    return df

def process_deliverect_remove_duplicates(df):
    # Create Custom Sort, to delete duplicates. Created a hierarchy for OrderStatus, as the first record will be retained
    custom_order = {'Delivered': 0, 'Auto Finalized': 1, 'In Delivery': 2, 'Ready For Pickup': 3, 'Prepared': 4, 'Preparing': 5, 'Accepted': 6, 'Deliverect Parsed': 7, 'New': 8, 'Scheduled': 9,
                    'Cancel': 10, 'Canceled': 11, 'Failed Resolved': 12, 'Failed': 13, 'Delivery Cancelled': 14, 'Manual Retry': 15, 'Failed Cancel': 16}
    df['OrderStatus'] = pd.Categorical(df['OrderStatus'], categories=custom_order.keys(), ordered=True)
    df = df.sort_values(by=['PrimaryKey', 'OrderStatus'], ascending=[True, True])

    # Keep only the first record for each unique 'PrimaryKey'
    df = df.drop_duplicates(subset='PrimaryKey', keep='first')
    df = df[df['OrderID'] != '#nan']
    return df

def clean_deliverect_product_name(df):
    # Replace NaN values in 'ProductName' with an empty string
    df['ProductName'] = df['ProductName'].fillna('')

    # Correct character encoding issues in 'ProductName'
    # Encoding and then decoding with 'latin-1' helps fix any special characters or encoding errors
    df.loc[:, 'ProductName'] = df['ProductName'].apply(lambda x: x.encode('latin-1', errors='ignore').decode('utf-8', errors='ignore'))

    # Replace specific abbreviations and phrases in 'ProductName' for consistency and clarity
    # For example, replace 'Stck' with 'Stack', 'Kse' with 'Cheese', and standardize variations of the word 'Hot'
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Stck', 'Stack')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Kse', 'Cheese')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('HOT HOT HOT', 'Hot')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace(' Hot Hot Hot', 'Hot')

    # Remove unnecessary commas and standardize product descriptions in 'ProductName'
    # This helps in maintaining uniformity and readability in product names
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Mayonnaise, 17ml', 'Mayonnaise 17ml')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Mayo, 50ml', 'Mayo 50ml')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Sauce, 50ml', 'Sauce 50ml')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Cola 0,5l', 'Cola 0.5l')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Salsa, 30ml', 'Salsa 30ml')
    df.loc[:, 'ProductName'] = df['ProductName'].str.replace('Italien,', 'Italien')
    return df

# Path to the Deliverect source folder containing order details and documents
deliverect_source_folder = r'H:\Shared drives\97 - Finance Only\01 - Orders & 3PL Documents\00 - All Restaurants\00 - Deliverect'

def load_deliverect_order_data():
    # Change the working directory to the 'Order Details' folder in the Deliverect source folder
    root_directory = deliverect_source_folder + r'\Order Details'
    os.chdir(root_directory)

    # Initialize an empty list to store individual dataframes loaded from each CSV file
    dataframe = []

    # Iterate over all files in the 'Order Details' directory
    for file_name in os.listdir(root_directory):
        # Check if the file is a CSV file and contains 'Orders' in its name
        if file_name.endswith(".csv") and 'Orders' in file_name:
            file_path = os.path.join(root_directory, file_name)
            # Load the CSV file into a dataframe, ensuring 'OrderID' is treated as a string
            df = pd.read_csv(file_path, dtype={'OrderID': str}, encoding='utf-8')
            # Append the loaded dataframe to the list
            dataframe.append(df)

    # Combine all dataframes into a single dataframe, if any CSV files were loaded
    if dataframe:
        df = pd.concat(dataframe, ignore_index=True)
    else:
        # Create an empty DataFrame if no CSV files were found in the directory
        df = pd.DataFrame()

    # Filter and retain only the essential columns
    df = df[['PickupTimeUTC', 'CreatedTimeUTC', 'ScheduledTimeUTC', 'Location', 'OrderID', 'Channel',
             'Status', 'Type', 'Payment', 'PaymentAmount', 'DeliveryCost', 'DiscountTotal', 'DriverTip',
             'SubTotal', 'Brands', 'IsTestOrder', 'ProductPLUs', 'ProductNames', 'OrderTotalAmount']]

    # Standardize column names to align with naming conventions for better readability
    df = df.rename(columns={
        'PickupTimeUTC': 'OrderPlacedDateTime',
        'CreatedTimeUTC': 'CourierDepartureFromRxDateTime',
        'ScheduledTimeUTC': 'OrderScheduledDateTime',
        'Status': 'OrderStatus',
        'Type': 'DeliveryType',
        'Payment': 'PaymentType',
        'DeliveryCost': 'DeliveryCost',
        'DiscountTotal': 'PromotionsOnItems',
        'DriverTip': 'Tip',
        'SubTotal': 'GrossAOV',
        'Brands': 'Brand',
        'IsTestOrder': 'IsTestOrder',
        'ProductPLUs': 'ProductPLU',
        'ProductNames': 'ProductName',
        'OrderTotalAmount': 'TotalOrderAmount'})

    # Apply 'process_deliverect_shared_data' and 'clean_deliverect_product_name' functions to DataFrame 'df'
    df = process_deliverect_shared_data(df)
    df = clean_deliverect_product_name(df)
    df = process_deliverect_remove_duplicates(df)

    # Convert and standardize the 'CourierDepartureFromRxDateTime' to UTC, then to 'Europe/Berlin' timezone
    # This ensures consistency in time representation across the dataset
    # Additionally, separate the date and time components for more granular analysis
    # This field represents the time when the courier departs with the order
    df['CourierDepartureFromRxDateTime'] = pd.to_datetime(df['CourierDepartureFromRxDateTime']).dt.tz_localize('UTC')
    df['CourierDepartureFromRxDateTime'] = df['CourierDepartureFromRxDateTime'].dt.tz_convert('Europe/Berlin')
    df['CourierDepartureFromRxDate'] = df['CourierDepartureFromRxDateTime'].dt.date
    df['CourierDepartureFromRxTime'] = df['CourierDepartureFromRxDateTime'].dt.time

    # Perform similar conversion for 'OrderScheduledDateTime'
    # Use try-except block to handle any potential conversion errors
    # If conversion fails, set 'ScheduledOrderTime' to NaN to indicate missing or invalid data
    try:
        df['OrderScheduledDateTime'] = pd.to_datetime(df['OrderScheduledDateTime']).dt.tz_localize('UTC')
        df['OrderScheduledDateTime'] = df['OrderScheduledDateTime'].dt.tz_convert('Europe/Berlin')
        df['ScheduledOrderDate'] = df['OrderScheduledDateTime'].dt.date
        df['ScheduledOrderTime'] = df['OrderScheduledDateTime'].dt.time
    except (ValueError, TypeError):
        df['ScheduledOrderTime'] = np.nan

    # Reorder DataFrame columns for better organization and readability
    # The columns are categorized and ordered based on their type and relevance to the data analysis
    first_cols = ['PrimaryKey', 'OrderID', 'Location', 'LocWithBrand', 'Brand', 'DeliveryType', 'Channel']  # Text Fields consistent across all data
    order_date_time_cols = ['OrderPlacedDate', 'OrderPlacedTime']  # Date and Time that the order was placed
    second_cols = ['OrderStatus', 'PaymentType']  # Other Text Fields (not in first_cols)
    boolean_cols = ['IsTestOrder']  # Boolean Columns, set to TRUE / FALSE
    financial_cols = ['GrossAOV', 'PromotionsOnItems', 'DeliveryCost', 'Tip']  # Financial Data
    numerical_cols = []  # Numerical Data (not financial)
    date_cols = []  # Date in yyyy-mm-dd format (Not Order Placed)
    time_cols = ['CourierDepartureFromRxTime']  # Time in hh:mm:ss (Not Order Placed)
    timevalue_cols = []  # Time Duration in hh:mm:ss
    last_cols = ['ProductPLU', 'ProductName']  # Data to place at the end of the dataframe, can be any type
    df = df[first_cols + order_date_time_cols + second_cols + financial_cols + numerical_cols + boolean_cols + date_cols + time_cols + timevalue_cols + last_cols]

    # Reset the DataFrame index to maintain sequential order after sorting and reordering columns
    # 'drop=True' ensures that the old index is not added as a column in the DataFrame
    df = df.reset_index(drop=True)

    # Return the concatenated dataframe or an empty dataframe if no CSV files were found
    return df

load_deliverect_order_data()

def load_deliverect_item_level_detail_data():
    # Change the working directory to the 'Order Details' folder in the Deliverect source folder
    root_directory = deliverect_source_folder + r'\Order Level Pricing'
    os.chdir(root_directory)

    # Initialize an empty list to store individual dataframes loaded from each CSV file
    dataframe = []

    # Iterate over all files in the 'Order Details' directory
    for file_name in os.listdir(root_directory):
        # Check if the file is a CSV file and contains 'Orders' in its name
        if file_name.endswith(".csv") and 'Order Level Pricing' in file_name:
            file_path = os.path.join(root_directory, file_name)
            # Load the CSV file into a dataframe, ensuring 'OrderID' is treated as a string
            df = pd.read_csv(file_path, dtype={'OrderID': str}, encoding='utf-8', low_memory=False)
            # Append the loaded dataframe to the list
            dataframe.append(df)

    # Combine all dataframes into a single dataframe, if any CSV files were loaded
    if dataframe:
        df = pd.concat(dataframe, ignore_index=True)
    else:
        # Create an empty DataFrame if no CSV files were found in the directory
        df = pd.DataFrame()

    # Filter and retain only the essential columns
    df = df[['CreatedTimeUTC', 'Location', 'OrderID', 'Channel', 'Status', 'Type', 'Payment', 'PaymentAmount', 'DeliveryCost', 'DiscountTotal', 'DriverTip', 'SubTotal', 'Brands', 'IsTestOrder', 'ProductPLUs',
             'ProductNames', 'OrderTotalAmount', 'ItemPrice', 'ItemQuantities']]

    # Standardize column names to align with naming conventions for better readability
    df = df.rename(columns={
        'CreatedTimeUTC': 'OrderPlacedDateTime',
        'Status': 'OrderStatus',
        'Type': 'DeliveryType',
        'Payment': 'PaymentType',
        'DeliveryCost': 'DeliveryCost',
        'DiscountTotal': 'PromotionsOnItems',
        'DriverTip': 'Tip',
        'SubTotal': 'GrossAOV',
        'Brands': 'Brand',
        'IsTestOrder': 'IsTestOrder',
        'ProductPLUs': 'ProductPLU',
        'ProductNames': 'ProductName',
        'OrderTotalAmount': 'TotalOrderAmount'})

    # Apply 'process_deliverect_shared_data' and 'clean_deliverect_product_name' functions to DataFrame 'df'
    df = process_deliverect_shared_data(df)
    df = clean_deliverect_product_name(df)

    # Load and align DataFrames
    # Load the deliverect order data and select relevant columns
    clean_df = load_deliverect_order_data()
    clean_df = clean_df[['PrimaryKey', 'OrderStatus']]

    # Create a temporary column 'TempColumn' to mark records from the 'clean_df' DataFrame
    clean_df['TempColumn'] = 'Orders'

    # Merge DataFrames and exclude records
    # Merge the 'df' DataFrame with 'clean_df' on 'PrimaryKey' and 'OrderStatus', keeping all rows
    df = pd.merge(df, clean_df[['PrimaryKey', 'OrderStatus', 'TempColumn']], on=['PrimaryKey', 'OrderStatus'], how='left')

    # Filter records that have a blank value in 'TempColumn', indicating they are not present in 'clean_df'
    df = df.loc[df['TempColumn'].notna()]

    # Create New ProductName and PLU, with price included
    df['ItemPrice'] = df['ItemPrice'] / 100
    df['CleanedProductPLU'] = df['ProductPLU'] + ' :' + df['ItemQuantities'].astype(str)
    df['CleanedProductName'] = df['ItemQuantities'].astype(str) + 'x ' + df['ProductName'].astype(str) + ' ' + (df['ItemPrice'] / 100).astype(str)

    # Reorder DataFrame columns for better organization and readability
    # The columns are categorized and ordered based on their type and relevance to the data analysis
    first_cols = ['PrimaryKey', 'OrderID', 'Location', 'LocWithBrand', 'Brand', 'DeliveryType', 'Channel']  # Text Fields consistent across all data
    order_date_time_cols = ['OrderPlacedDate', 'OrderPlacedTime']  # Date and Time that the order was placed
    second_cols = ['OrderStatus', 'PaymentType']  # Other Text Fields (not in first_cols)
    boolean_cols = ['IsTestOrder']  # Boolean Columns, set to TRUE / FALSE
    financial_cols = ['GrossAOV', 'PromotionsOnItems', 'DeliveryCost', 'Tip', 'TotalOrderAmount']  # Financial Data
    numerical_cols = []  # Numerical Data (not financial)
    date_cols = []  # Date in yyyy-mm-dd format (Not Order Placed)
    time_cols = []  # Time in hh:mm:ss (Not Order Placed)
    timevalue_cols = []  # Time Duration in hh:mm:ss
    last_cols = ['ProductPLU', 'ProductName', 'ItemPrice', 'ItemQuantities']  # Data to place at the end of the dataframe, can be any type
    df = df[first_cols + order_date_time_cols + second_cols + financial_cols + numerical_cols + boolean_cols + date_cols + time_cols + timevalue_cols + last_cols]

    # Reset the DataFrame index to maintain sequential order after sorting and reordering columns
    # 'drop=True' ensures that the old index is not added as a column in the DataFrame
    df = df.reset_index(drop=True)

    # TODO: De Dupe Order 865 from Friedrichshain in Jan 23

    # Create Item Level Pricing by multiplying 'ItemPrice' and 'ItemQuantities'
    df['TotalPrice'] = df['ItemPrice'] * df['ItemQuantities']

    # Create a copy of the DataFrame with selected columns for consolidation
    consolidated_df = df[
        ['PrimaryKey', 'ProductName', 'ProductPLU', 'ItemQuantities', 'ItemPrice', 'TotalPrice', 'GrossAOV']].copy()

    # Define a custom aggregation function to extract the first 'GrossAOV' value within each 'PrimaryKey' group
    def first_total_order_amount(series):
        return series.iloc[0]

    # Group the data by 'PrimaryKey', calculate the sum of 'TotalPrice', and apply the custom aggregation function to 'GrossAOV'
    consolidated_df = consolidated_df.groupby('PrimaryKey').agg(
        {'TotalPrice': 'sum', 'GrossAOV': first_total_order_amount}).reset_index()

    # Check if 'GrossAOV' and 'TotalPrice' are within a specified tolerance to identify reconciliation discrepancies
    consolidated_df['Check'] = abs(
        consolidated_df['GrossAOV'] - consolidated_df['TotalPrice']) < 0.001  # Define your tolerance here

    # Filter and keep only the rows where the reconciliation check failed
    consolidated_df = consolidated_df.loc[consolidated_df['Check'] == False]  # Change 'False' to False

    # Initialize an empty list to store the new records
    new_records = []

    # Iterate through each row in the filtered 'consolidated_df' and create a new record for each
    for index, row in consolidated_df.iterrows():
        new_record = {
            'PrimaryKey': row['PrimaryKey'],  # Copy the 'PrimaryKey' value
            'ProductName': 'Balancing Item',
            'ProductPLU': 'x-xx-xxx-x',
            'ItemQuantities': 1,
            'ItemPrice': row['GrossAOV'] - row['TotalPrice'],
            'TotalPrice': row['GrossAOV'] - row['TotalPrice']
        }

        # Fill additional columns from the corresponding row in the original DataFrame 'df'
        matching_row = df[df['PrimaryKey'] == row['PrimaryKey']].iloc[0]
        for column in ['OrderID', 'Location', 'LocWithBrand', 'Brand', 'DeliveryType', 'Channel', 'OrderPlacedDate',
                       'OrderPlacedTime', 'OrderStatus', 'PaymentType', 'GrossAOV', 'PromotionsOnItems',
                       'DeliveryCost', 'Tip', 'TotalOrderAmount', 'IsTestOrder']:
            new_record[column] = matching_row[column]

        new_records.append(new_record)

    # Append the new records to the original DataFrame 'df'
    df = pd.concat([df, pd.DataFrame(new_records)], ignore_index=True)

    # Sort the DataFrame by 'OrderPlacedDate', 'OrderPlacedTime', and 'PrimaryKey' to meet your sorting requirements
    df.sort_values(['OrderPlacedDate', 'OrderPlacedTime', 'PrimaryKey'], inplace=True)

    # Reset the index to maintain sequential order
    df = df.reset_index(drop=True)

    # Return the concatenated DataFrame or an empty DataFrame if no reconciliation discrepancies were found
    return df


load_deliverect_item_level_detail_data()