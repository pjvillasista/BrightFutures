# Data Ingestion and Analysis Architecture

This document outlines the architecture designed to scrape data from websites, analyze the sentiment of reviews, clean the data for storage, and ingest it into Snowflake for further processing and analysis.

## Overview

The process involves several key components and steps:

1. **Data Collection**: Utilize Python scripts to scrape data from websites.
2. **Data Analysis**: Perform sentiment analysis on the scraped reviews using Python.
3. **Data Cleaning**: Clean the data for the schools, preparing it for storage.
4. **Data Storage**: Store the raw and processed data in separate Amazon S3 buckets.
5. **Data Ingestion**: Use Snowflake's Snowpipe feature to automatically ingest the data from S3 into Snowflake.

## Detailed Steps

### 1. Data Collection

- Python scripts are employed to scrape necessary data from target websites. 
- The raw data collected during this phase includes textual reviews and various metadata related to schools.

### 2. Data Analysis

- The raw reviews undergo sentiment analysis through Python scripts, determining the polarity and subjectivity of each review.
- The outcome categorizes reviews into positive, neutral, or negative sentiments.

### 3. Data Cleaning

- Following analysis, the data is cleaned and formatted. This includes structuring the data into CSV format, ensuring consistency, and preparing it for storage.
- Separate processes are used for cleaning the school metadata and the sentiment analysis results.

### 4. Data Storage

- Cleaned data is stored in designated Amazon S3 buckets. There are two main types of data stored:
  - **Geoencoded Schools Data**: Contains cleaned information about schools.
  - **Reviews Sentiment Analysis Data**: Contains results from the sentiment analysis of reviews.
- These data are stored in specific folders within the S3 buckets, ready for ingestion into Snowflake.

### 5. Data Ingestion into Snowflake

The Snowflake setup involves several components and steps:

#### Creating Database and Schema

- A database `BRIGHTFUTURES` and a schema `STAGING` within it are created to organize the ingested data.

#### Defining File Formats

- File formats for CSV files are defined to specify how the data files should be parsed during ingestion.

#### Creating External Stages

- External stages are created for the `geoencoded schools` and `reviews sentiment analysis` data. These stages point to the corresponding folders in the S3 buckets.

#### Listing Files in Stages

- Commands are provided to list the files in the external stages, ensuring visibility of the data ready for ingestion.

#### Setting Up Snowpipe

- Snowpipe configurations are created for continuous data loading from the S3 buckets into Snowflake. This automates the process of moving data into specific tables within Snowflake as soon as it's available in the stages.

### Conclusion

This architecture facilitates an efficient and automated pipeline for collecting, analyzing, cleaning, and ingesting data into Snowflake. By leveraging Python for data collection and analysis, and Snowflake's Snowpipe for seamless data ingestion, we achieve a robust solution for managing and analyzing web-scraped data.
