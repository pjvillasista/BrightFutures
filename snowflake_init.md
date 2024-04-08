# Snowflake Setup for BrightFutures Project

This document outlines the steps to create a Snowflake database environment for the BrightFutures project. The process involves setting up databases, schemas, file formats, stages, and Snowpipes for efficient data handling and analytics.

## Creating Database and Schemas

### Database Creation

```sql
CREATE DATABASE IF NOT EXISTS BRIGHTFUTURES;
```

**Why?** Ensures the `BRIGHTFUTURES` database exists for project data. Using `IF NOT EXISTS` avoids errors if the database already exists, facilitating idempotent scripts.

### Schema Creation

```sql
CREATE SCHEMA BRIGHTFUTURES.STAGING;
```

**Why?** Creates a `STAGING` schema under the `BRIGHTFUTURES` database for temporary storage of raw data before transformation. This is a crucial step in data processing pipelines, allowing for initial data validation and cleaning.

## File Format Object

### CSV File Format

```sql
-- Create a file format for CSV files
CREATE FILE FORMAT IF NOT EXISTS BRIGHTFUTURES.STAGING.format_csv
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1;
```

**Why?** Specifies the format for CSV files to ensure consistent data parsing during the ingest process. Settings like `FIELD_DELIMITER`, `RECORD_DELIMITER`, and `SKIP_HEADER` are critical for correctly interpreting the data structure.

## Creating External Stages

External stages are defined to reference raw data stored in cloud storage, allowing Snowflake to access and ingest this data.

### Geoencoded Schools Stage

```sql
CREATE OR REPLACE STAGE BRIGHTFUTURES.STAGING.geoencoded_schools_stage
    URL='s3://brightfutures-school-info-raw/staging/geoencoded_schools/'
    CREDENTIALS=(aws_key_id='' aws_secret_key='')
    FILE_FORMAT = BRIGHTFUTURES.STAGING.format_csv;
```

### Reviews Sentiment Stage

```sql
CREATE OR REPLACE STAGE BRIGHTFUTURES.STAGING.reviews_sentiment_stage
    URL='s3://brightfutures-school-info-raw/staging/reviews_sentiment/'
    CREDENTIALS=(aws_key_id='' aws_secret_key='')
    FILE_FORMAT = BRIGHTFUTURES.STAGING.format_csv;
```

**Why?** Stages link Snowflake to the S3 buckets containing raw data. They are essential for the automated data ingestion pipeline, enabling Snowflake to efficiently import and process large datasets.

## Listing Files in Stages

```sql
-- List files for geoencoded schools
LIST @BRIGHTFUTURES.STAGING.geoencoded_schools_stage;

-- List files for reviews sentiment analysis
LIST @BRIGHTFUTURES.STAGING.reviews_sentiment_stage;
```

**Why?** Lists the files available in each external stage. This is useful for verifying that Snowflake can access the raw data files and for troubleshooting any issues with file paths or permissions.

## Snowpipe for Continuous Data Ingestion

Snowpipe allows for the near-real-time loading of data into Snowflake, automating the ingestion process.

### Schema for Snowpipe

```sql
CREATE OR REPLACE SCHEMA BRIGHTFUTURES.SNOWPIPE_SCHEMA;
```

**Why?** Separates Snowpipe definitions from other database objects for organizational clarity and management.

### Geoencoded Schools Pipe

```sql
CREATE OR REPLACE PIPE BRIGHTFUTURES.SNOWPIPE_SCHEMA.geoencoded_schools_pipe
AUTO_INGEST = TRUE
AS
COPY INTO BRIGHTFUTURES.DATA_SCHEMA.GEOENCODED_SCHOOLS
FROM @BRIGHTFUTURES.STAGING.geoencoded_schools_stage
FILE_FORMAT = (FORMAT_NAME = BRIGHTFUTURES.STAGING.format_csv);
```

### Reviews Sentiment Pipe

```sql
CREATE OR REPLACE PIPE BRIGHTFUTURES.SNOWPIPE_SCHEMA.reviews_sentiment_pipe
AUTO_INGEST = TRUE
AS
COPY INTO BRIGHTFUTURES.DATA_SCHEMA.REVIEWS_SENTIMENT
FROM @BRIGHTFUTURES.STAGING.reviews_sentiment_stage
FILE_FORMAT = (FORMAT_NAME = BRIGHTFUTURES.STAGING.format_csv);
```

**Why?** Snowpipes automate the data load from external stages into Snowflake tables. `AUTO_INGEST = TRUE` enables automatic data ingestion upon file arrival in the stage, crucial for timely data analysis and insights.

## Data Schema and Tables

### Data Schema Creation

```sql
CREATE SCHEMA IF NOT EXISTS BRIGHTFUTURES.DATA_SCHEMA;
```

**Why?** Defines a dedicated schema for cleaned and transformed data, separating it from raw staging data. This is

a best practice for data organization and access control.

### Tables for Data Storage

#### Geoencoded Schools Table

```sql
CREATE TABLE IF NOT EXISTS BRIGHTFUTURES.DATA_SCHEMA.GEOENCODED_SCHOOLS ( ... );
```

#### Reviews Sentiment Table

```sql
CREATE TABLE IF NOT EXISTS BRIGHTFUTURES.DATA_SCHEMA.REVIEWS_SENTIMENT ( ... );
```

**Why?** Creating specific tables for different data types ensures structured data storage, facilitating efficient querying and analysis. Each table's schema is designed to hold relevant data attributes, enabling detailed data insights.
