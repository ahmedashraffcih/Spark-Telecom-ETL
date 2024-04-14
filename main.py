import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Telecom ETL").getOrCreate()

from pyspark.sql.types import *
from pyspark.sql.functions import *

#### Define schema for the DataFrame

schema = StructType([StructField("imsi",StringType(), False),
                     StructField("imei",StringType(), True),
                     StructField("cell",IntegerType(), False),
                     StructField("lac",IntegerType(), False),
                     StructField("eventType",StringType(), True),
                     StructField("eventTs",TimestampType(), False),
                     ])

#### Read data with specified schema and delimiter

df = spark.read.option("delimiter","|")\
.option("header",'false')\
.csv("ercsn_4g_20200512182929_part02.csv_processing",schema=schema)\
.withColumn("filename",lit("someFileName"))
df.show(truncate=False)
#### Filter rejected records
rejected_df = df.filter((col("imsi").isNull()) | (col("cell").isNull()) | (col("lac").isNull())| (col("eventTs").isNull()))
rejected_df.show()
#### Derive new columns and filter valid records
new_cols_df = df.withColumn("tac", 
                            when(col("imei").isNull() | (length(col("imei")) < 15), "-99999")
                            .otherwise(substring(col("imei"), 0, 7))) \
                .withColumn("snr", 
                            when(col("imei").isNull() | (length(col("imei")) < 15), "-99999")
                            .otherwise(substring(col("imei"), 8, 13)))\
                .withColumn("event_ts",
                            to_timestamp(col("eventTs"),'YYYY-mm-dd HH:MM:SS'))\
                .filter((col("imsi").isNotNull()) & (col("cell").isNotNull()) & (col("lac").isNotNull()) & (col("eventTs").isNotNull()))

# Drop redundant columns
df_dropped = new_cols_df.drop("eventTs")
df_dropped.show()

#### Show counts

print(f"Total Number of records: {df.count()}")
print(f"Total Number of Accepted records: {df_dropped.count()}")
print(f"Total Number of rejected records: {rejected_df.count()}")
print(f"Total Number of processed records: {rejected_df.count() + df_dropped.count()}")

#### Select final columns
final_df = df_dropped.select("imsi","tac","snr","imei","cell","lac","eventType","event_ts","filename")

final_df.show()

#### Convert Spark DataFrame to Pandas DataFrame
##### Save the Pandas DataFrame to a CSV file
import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
pandas_df = final_df.toPandas()

# Save the Pandas DataFrame to a CSV file
pandas_df.to_csv("C:/Users/Ahmed Ashraf/Desktop/Telecom-ETL/Final_data/processed.csv", index=False)
##### Stop the SparkSession
spark.stop()