import pandas as pd
import datetime as dt
import os
from datetime import datetime
from datetime import timedelta

# Get All Sub Folders within a folder
def get_immediate_subfolders(directory_path):
   return [os.path.join(directory_path, d) for d in os.listdir(directory_path) if
           os.path.isdir(os.path.join(directory_path, d))]

# Fix Issue Where Order ID's with an E as the 3rd character comes in as a scientific number
def convert_to_custom_format(scientific_notation):
   try:
       coefficient, exponent = scientific_notation.split('E+')
       coefficient = coefficient.replace('.', '')
       new_exponent = int(exponent) - 2
       return f"{coefficient}E{new_exponent}"
   except ValueError:
       # Handle non-convertible values here (e.g., return the original value)
       return scientific_notation

# Clean Rx Names to the defined list
def clean_location_names(df):
   # Clean Location
   cleaned_names = pd.read_csv(r'H:\Shared drives\97 - Finance Only\10 - Cleaned Data\01 - Restaurant Names\Full Rx List, with Cleaned Names.csv')
   df['Location'] = df['Location'].str.replace('ö', 'o').str.replace('ü', 'u')
   df = pd.merge(df, cleaned_names[['Location', 'Cleaned Name']], on='Location', how='left')
   df = df.rename(columns={'Cleaned Name': 'Cleaned Location'})
   df['Location'] = df['Cleaned Location']
   df = df.drop(columns=['Cleaned Location'])
   return df

def convert_time_format(time_decimal):  # This converts for decimal time to hh:mm:ss
    if pd.notnull(time_decimal):
        total_seconds = int(time_decimal * 60)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = (total_seconds % 60)
        time_obj = dt.time(hours, minutes, seconds)
        return time_obj.strftime('%H:%M:%S')
    else:
        return ''

def format_timedelta(td):
    if pd.notnull(td):
        hours, remainder = divmod(td.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    else:
        return ''

def week_start(date):
    date = pd.to_datetime(date)
    days_since_monday = date.weekday()
    week_start = date - dt.timedelta(days=days_since_monday)
    return week_start

def month_start(date):
    return pd.to_datetime(date).to_period('M').to_timestamp()

def get_period_string(date_obj):
    # Determine period based on day of the month
    period_num = 1 if date_obj.day <= 15 else 2

    # Format period string as "P{period_num} mmm yy"
    period_str = f"P{period_num} {date_obj.strftime('%b %y')}"
    return period_str