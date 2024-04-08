import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderUnavailable
from concurrent.futures import ThreadPoolExecutor, as_completed
import ast

# Constants
BUCKET_NAME = 'brightfutures-school-info-raw'
RAW_PREFIX = 'raw/school_info'
STAGING_PREFIX = 'staging/geoencoded_schools'
s3_resource = boto3.resource('s3')


def get_latest_file_name(bucket_name, prefix):
    """
    Get the most recent file from a specified S3 bucket and prefix
    """
    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', [])]
    latest_file = max(files, key=lambda x: x.split('/')[-1])
    return latest_file

def read_csv_from_s3(bucket_name, key):
    """
    Load a CSV file from S3 into a DataFrame
    """
    obj = s3_resource.Object(bucket_name, key)
    data = obj.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))
    return df

def save_df_to_s3(df, bucket_name, prefix, file_name):
    """
    Save a DataFrame to an S3 bucket with the specified prefix and file name
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket_name, f'{prefix}/{file_name}').put(Body=csv_buffer.getvalue())


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
    

def preprocess_grade_levels(df):
    # Initialize new columns as False
    df['IsPreK'] = False
    df['IsElementary'] = False
    df['IsMiddle'] = False
    df['IsHigh'] = False

    # For each row, check "School Types" and update the new columns accordingly
    for index, row in df.iterrows():
        # Ensure school types are correctly interpreted as lists
        try:
            school_types = ast.literal_eval(row['School Types'])
        except ValueError:
            school_types = []
            
        if 'Pre-K' in school_types:
            df.at[index, 'IsPreK'] = True
        if 'Elementary school' in school_types:
            df.at[index, 'IsElementary'] = True
        if 'Middle school' in school_types:
            df.at[index, 'IsMiddle'] = True
        if 'High school' in school_types:
            df.at[index, 'IsHigh'] = True

def normalize_and_categorize(df):
    # Normalize specified columns
    columns_to_normalize = ["Test Scores", "Academic Progress", "Equity Scores"]
    for col in columns_to_normalize:
        df[f"Normalized {col}"] = df[col].astype(float) / 10

    # Calculate the composite score
    df["Composite Score"] = (
        df[["Normalized Test Scores", "Normalized Academic Progress", "Normalized Equity Scores"]]
        .mean(axis=1) * 9 + 1
    ).round()

    # Function to categorize composite score
    def categorize_score(score):
        if pd.isna(score):
            return "Data Not Available"
        elif 1 <= score <= 4:
            return "Below Average"
        elif 5 <= score <= 6:
            return "Average"
        else:
            return "Above Average"

    # Apply the categorization function
    df["Score Category"] = df["Composite Score"].apply(categorize_score)

def preprocess_and_geoencode(df):
    preprocess_grade_levels(df)
    normalize_and_categorize(df)
    
    # Assuming 'School Types' column is a string representation of lists
    df['School Types'] = df['School Types'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else [])

    geolocator = Nominatim(user_agent="geo_encoding_schools", timeout=10)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    def geocode_address(address):
        try:
            location = geocode(address)
            if location:
                return (location.latitude, location.longitude)
            else:
                return (None, None)
        except Exception as e:
            print(f"Error geocoding {address}: {e}")
            return (None, None)

    # Geocode addresses (this could be slow and should respect the service's rate limits)
    df['Coordinates'] = df['Address'].apply(geocode_address)

    # Split coordinates into separate columns
    df[['Latitude', 'Longitude']] = pd.DataFrame(df['Coordinates'].tolist(), index=df.index)
    
    return df

# Main processing function
def main():
    # Determine the latest file in the raw data folder
    latest_file_key = get_latest_file_name(BUCKET_NAME, RAW_PREFIX)
    
    # Read the latest raw file into a DataFrame
    df = read_csv_from_s3(BUCKET_NAME, latest_file_key)
    
    # Preprocess and geoencode the data
    processed_df = preprocess_and_geoencode(df)
    
    # Save the processed data to the staging area
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d%H%M%S")
    staging_file_name = f"geoencoded_schools_{timestamp}.csv"
    staging_prefix = f"{STAGING_PREFIX}/{now.year}/{now.month:02d}/{now.day:02d}"
    save_df_to_s3(processed_df, BUCKET_NAME, staging_prefix, staging_file_name)

if __name__ == "__main__":
    main()
