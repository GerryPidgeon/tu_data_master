import pandas as pd
import numpy as np
import os
import sys

# Update system path to include parent directories for module access
# This allows the script to import modules from two directories up in the folder hierarchy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import specific data and functions from external modules
from deliverect._01_import_deliverect import imported_deliverect_item_level_detail_data

def process_deliverect_shared_data():
    # Change the working directory to the 'Order Details' folder in the Deliverect source folder
    df = imported_deliverect_item_level_detail_data

    rebuild_df = df.copy()
    print(rebuild_df)

    return df

process_deliverect_shared_data()

