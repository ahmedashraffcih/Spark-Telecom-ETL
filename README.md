# Telecom ETL Process

This script is designed to perform ETL (Extract, Transform, Load) operations on telecom data using Apache Spark.

## Prerequisites

Before running the script, ensure you have the following installed:

- Apache Spark
- Python (with PySpark)
- Java

## Usage

1. Ensure that your environment is set up correctly with Apache Spark installed and configured.
2. Replace the file path in the script (`ercsn_4g_20200512182929_part02.csv_processing`) with the path to your telecom data file.
3. Run the script using a Python interpreter.

## Description

1. The script reads telecom data from a CSV file, specifying a custom schema to parse the columns correctly.
2. It filters out rejected records where essential fields (imsi, cell, lac, eventTs) are missing.
3. New columns, 'tac' and 'snr', are derived from the 'imei' field. If 'imei' is missing or shorter than 15 characters, '-99999' is assigned to these columns.
4. Redundant columns ('eventTs') are dropped.
5. Statistics are printed regarding the number of total records, accepted records, rejected records, and processed records.
6. The final DataFrame is created with selected columns ('imsi', 'tac', 'snr', 'imei', 'cell', 'lac', 'eventType', 'event_ts', 'filename').
7. The Spark DataFrame is converted to a Pandas DataFrame for easier handling.
8. The Pandas DataFrame is saved to a CSV file named `processed.csv` in the specified directory (`Telecom-ETL/Final_data/`).

## Notes

- Make sure to adjust the file paths and column names as per your specific data and requirements.
- This script assumes that Spark is configured and initialized correctly.