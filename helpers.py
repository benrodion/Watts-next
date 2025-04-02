import pandas as pd
import numpy as np

# Fix function
def fix_datetime_format(date_str):
    parts = date_str.split("-")
    year, month, day, hour = map(int, parts)  # Convert all parts to integers
    
    if hour == 24:
        hour = 0  # Reset hour to 00
        new_date = pd.Timestamp(year, month, day) + pd.Timedelta(days=1)  # Add a day
        return new_date.replace(hour=hour)  # Set hour to 00
    else:
        return pd.Timestamp(year, month, day, hour)



# a function that creates 3D data that we need 
def create_sliding_windows(data, x_len=400, y_len=25, step=None):
    """
    Create non-overlapping windows of X and Y from a time series.
    
    Parameters:
        data (array-like): the full dataset 
        x_len (int): Length of input sequence
        y_len (int): Length of target sequence
        step (int): Step size between windows (defaults to x_len + y_len)
    
    Returns:
        X_windows: List of input arrays
        Y_windows: List of output arrays
    """
    if step is None:
        step = x_len + y_len
    
    X_windows = []
    Y_windows = []

    max_start = len(data) - (x_len + y_len) #to stay within the limits of the data set 
    for start in range(0, max_start + 1, step):
        end_x = start + x_len
        end_y = end_x + y_len
        X_windows.append(data[start:end_x])
        Y_windows.append(data[end_x:end_y])
    
    return np.array(X_windows), np.array(Y_windows)