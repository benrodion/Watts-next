# -*- coding: utf-8 -*-
"""
Created on Sat Mar  8 22:55:39 2025

!!!
Run only if you need to collect and aggregate data from scratch, otherwise use 'avg_onshore_meteo_2017_2025.csv' and 'avg_offshore_meteo_2017_2025.csv' datasets from the repository
!!!

@author: Sofiya Berdiyeva
"""
import pandas as pd
import requests
import io
import time
import numpy as np
from scipy.stats import circmean

# Define parameters
base_url = "https://www.daggegevens.knmi.nl/klimatologie/uurgegevens"
stations = [209, 210, 215, 225, 235, 240, 242, 248, 249, 251, 257, 258, 260, 265, 267, 269, 270, 273, 275, 277,
            278, 279, 280, 283, 285, 286, 290, 308, 310, 311, 312, 313, 315, 316, 319, 323, 324, 330, 331, 340, 343,
            344, 348, 350, 356, 370, 375, 377, 380, 391]

start_date = "20170101"
end_date = "20250101"
variables = "DD:FH:FF:P:U"

# Looping through stations to get respective data

data_frames = []

for station in stations:
    print(f"Retrieving data for station {station}...")
    post_data = {
        "start": start_date,
        "end": end_date,
        "vars": variables,
        "stns": station,
        "fmt": "csv"
    }
    
    response = requests.post(base_url, data=post_data)
    time.sleep(1)  # Give server time to respond
    
    response_text = response.text
    
    # Remove comment lines
    data_lines = [line for line in response_text.split("\n") if not line.startswith("#") and line.strip()]
    
    # Create DataFrame
    if data_lines:
        df = pd.read_csv(
            io.StringIO("\n".join(data_lines)), 
            names=["STN", "YYYYMMDD", "HH", "DD", "FH", "FF", "P", "U"],
            dtype={"STN": int, "YYYYMMDD": int, "HH": int, "DD": str, "FH": str, "FF": str, "P": str, "U": str},
            low_memory=False
        )
        data_frames.append(df)
    time.sleep(3)  # Rest before next request

# Concatenate all stations data into one final DataFrame
final_df = pd.concat(data_frames, ignore_index=True)

# Display sample data
print(final_df.head())

# Clean data from tabulation and spaces
final_df['P'] = final_df['P'].str.replace(r'[\s\t]', '', regex=True)
final_df['U'] = final_df['U'].str.replace(r'[\s\t]', '', regex=True)

# Handle nulls with float types and optimize datatypes
final_df['P'] = final_df['P'].replace(r'^\s*$', float('nan'), regex=True).astype('float32')
final_df['U'] = final_df['U'].replace(r'^\s*$', float('nan'), regex=True).astype('float32')
final_df['DD'] = final_df['DD'].replace(r'^\s*$', float('nan'), regex=True).astype('float32')
final_df['FH'] = final_df['FH'].replace(r'^\s*$', float('nan'), regex=True).astype('float32')
final_df['FF'] = final_df['FF'].replace(r'^\s*$', float('nan'), regex=True).astype('float32')
final_df['HH'] = final_df['HH'].astype('int8')
final_df['STN'] = final_df['STN'].astype('int16')

# Short info on datatypes and null values
final_df.info()

final_df['U'].isnull().sum()
final_df['P'].isnull().sum()

# Some of the stations have wind direction equal to "990", which means it was too unstable during the hour to calculate the average. We delete these rows to not harm the further calculations
print(final_df[final_df["DD"] == 990].shape[0])
final_df = final_df[final_df["DD"] != 990]



# Off- and onshore aggregation

# List of offshore stations' numbers
offshore_stations = { 
    209, 210, 225, 235, 242, 248, 251, 257, 258, 267, 277, 285, 308, 312, 313, 316, 330
}

final_df["is_offshore"] = final_df["STN"].isin(offshore_stations)

# Saving full data
#final_df.to_csv('all_data_2017_2025.csv', index = False)

# Group and average while considering angular cyclicity
def aggregate_weather(df):
    aggregated_df = df.groupby(['YYYYMMDD', 'HH']).agg({
        'DD': lambda x: circmean(x.dropna(), high=360, low=0) if not x.dropna().empty else np.nan,
        'FH': 'mean',
        'FF': 'mean',
        'P': 'mean',
        'U': 'mean'
    }).reset_index()
    
    # Add full_datetime column directly with proper formatting
    aggregated_df['full_datetime'] = pd.to_datetime(aggregated_df['YYYYMMDD'].astype(str), format='%Y%m%d').dt.strftime('%Y-%m-%d') + '-' + aggregated_df['HH'].astype(str).str.zfill(2)
    
    return aggregated_df

# Compute averages for onshore and offshore
avg_onshore_meteo_df = aggregate_weather(final_df[final_df['is_offshore'] == False])

avg_offshore_meteo_df = aggregate_weather(final_df[final_df['is_offshore'] == True])

avg_onshore_meteo_df.info()
avg_offshore_meteo_df.info()

# Renaming the variables
column_names = ['year_mon_day', 'hour', 'wind_dir_avg_10', 'wind_speed_h_avg', 'wind_speed_avg_10', 'air_pressure', 'humidity', 'full_datetime']

avg_onshore_meteo_df.columns = column_names
avg_offshore_meteo_df.columns = column_names


# Saving data
#avg_onshore_meteo_df.to_csv('avg_onshore_meteo_2017_2025.csv', index = False)

#avg_offshore_meteo_df.to_csv('avg_offshore_meteo_2017_2025.csv', index = False)

# Load energy production data
energy_onshore_df = pd.read_csv("WindOnShore_data_2017_2025_clean.csv", index_col=0)
energy_offshore_df = pd.read_csv("WindOffShore_data_2017_2025_clean.csv", index_col=0)

# Explore variables in energy data
energy_onshore_df.info()

# Merge data
onshore_merged = pd.merge(avg_onshore_meteo_df, energy_onshore_df, left_on='full_datetime',right_on='correct_days')
offshore_merged = pd.merge(avg_offshore_meteo_df, energy_offshore_df, left_on='full_datetime',right_on='correct_days')

# Save data
onshore_merged.to_csv('final_onshore_data_2017_2025.csv', index = False)
offshore_merged.to_csv('final_offshore_data_2017_2025.csv', index = False)
