import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderUnavailable
from concurrent.futures import ThreadPoolExecutor, as_completed
import ast

# Load data
df = pd.read_csv('./data_test/raw/raw_school_data.csv')

# Preprocess and create boolean columns for grade levels
grade_levels = ['Pre-K', 'Elementary school', 'Middle school', 'High school']
for grade in grade_levels:
    df[f'Is{grade.replace(" ", "")}'] = df['School Types'].apply(lambda x: grade in x)

# Normalize specified columns
columns_to_normalize = ["Test Scores", "Academic Progress", "Equity Scores"]
for col in columns_to_normalize:
    df[f"Normalized {col}"] = df[col] / 10

# Calculate the composite score
df["Composite Score"] = df[[f"Normalized {col}" for col in columns_to_normalize]].mean(axis=1) * 9 + 1
df["Composite Score"] = df["Composite Score"].round()

# Function to categorize composite score
def categorize_score(score):
    if pd.isna(score):
        return "Data Not Available"
    elif score <= 4:
        return "Below Average"
    elif 5 <= score <= 6:
        return "Average"
    else:
        return "Above Average"

# Apply the categorization function
df["Score Category"] = df["Composite Score"].apply(categorize_score)

# Convert 'School Types' from string representation to actual lists
df['School Types'] = df['School Types'].apply(ast.literal_eval)

# Expanding school types into boolean columns
school_types = ['Private', 'Public district', 'Public charter']
for stype in school_types:
    df[stype] = df['School Types'].apply(lambda x: stype in x)

# Initialize the geocoder with rate limiter
geolocator = Nominatim(user_agent="geo_encoding_grouped", timeout=10)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, max_retries=2, error_wait_seconds=10)

# Function to geocode address
def geocode_address(address):
    try:
        location = geocode(address)
        return (location.latitude, location.longitude) if location else (None, None)
    except Exception as e:
        print(f"Error geocoding {address}: {e}")
        return (None, None)

# Batch geocode addresses
def batch_geocode(addresses, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(geocode_address, address) for address in addresses]
        return [future.result() for future in as_completed(futures)]

# Group by city and geocode
for city, group in df.groupby('City'):
    print(f"Processing city: {city}")
    coordinates = batch_geocode(group['Address'].tolist())
    latitudes, longitudes = zip(*coordinates) if coordinates else ([], [])
    df.loc[group.index, 'lat'] = latitudes
    df.loc[group.index, 'lon'] = longitudes
    print(f"Finished geocoding for {city}")

# Save the updated DataFrame
df.to_csv('./data/raw/stg_all_schools.csv', index=False)
