import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import ast
from snowflake.snowpark.session import Session


# Helper functions
def categorize_score(score):
    if pd.isna(score):
        return "Data Not Available"
    elif score <= 4:
        return "Below Average"
    elif 5 <= score <= 6:
        return "Average"
    else:
        return "Above Average"


def preprocess_and_geoencode(df):
    df["is_prek"] = df["SCHOOL_TYPES"].apply(lambda x: "Pre-K" in x)
    df["is_elementary"] = df["SCHOOL_TYPES"].apply(lambda x: "Elementary school" in x)
    df["is_middle"] = df["SCHOOL_TYPES"].apply(lambda x: "Middle school" in x)
    df["is_high"] = df["SCHOOL_TYPES"].apply(lambda x: "High school" in x)
    df["Score Category"] = df["GS_RATING"].apply(categorize_score)

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

    # Safely geocode addresses and ensure the list for DataFrame creation doesn't include NoneType objects
    coordinates_list = []
    for address in df["ADDRESS"]:
        result = geocode_address(address)
        coordinates_list.append(result if result != (None, None) else (pd.NA, pd.NA))

    # Create Latitude and Longitude columns from the coordinates list
    df[["Latitude", "Longitude"]] = pd.DataFrame(coordinates_list, index=df.index)

    df["transformed_at"] = pd.Timestamp.now(tz="UTC")
    df = df[
        [
            "SCHOOL_NAME",
            "ADDRESS",
            "GS_RATING",
            "ACADEMIC_PROGRESS",
            "TEST_SCORES",
            "EQUITY_SCORES",
            "SCHOOL_TYPES",
            "STAR_RATING",
            "REVIEW_LINK",
            "SCHOOL_LINK",
            "CITY",
            "is_prek",
            "is_elementary",
            "is_middle",
            "is_high",
            "Latitude",
            "Longitude",
            "transformed_at",
        ]
    ]
    return df


def snowpark_session_create():
    connection_params = {
        "account": "...",
        "user": "...",
        "password": "...",
        "role": "transform",
        "warehouse": "...",
        "database": "BRIGHTFUTURES",
        "schema": "STAGING",
    }
    session = Session.builder.configs(connection_params).create()
    return session


def load_data_to_snowflake(session, df, table_name):
    # Convert Pandas DataFrame to Snowpark DataFrame
    sp_df = session.create_dataframe(df)

    # Write the Snowpark DataFrame to a Snowflake table
    sp_df.write.save_as_table(table_name, mode="append", table_type="temporary")


def main():
    session = snowpark_session_create()
    df = session.table(
        "RAW.RAW_SCHOOL_INFO"
    ).to_pandas()  # Assuming your raw table is in the RAW schema
    processed_df = preprocess_and_geoencode(df)

    # Specify the target table within the STAGING schema
    table_name = "STAGING.STG_SCHOOL_INFO"
    session.create_dataframe(processed_df).write.mode("append").save_as_table(
        table_name
    )

    print(f"Data successfully loaded into {table_name}.")
    session.close()


if __name__ == "__main__":
    main()
