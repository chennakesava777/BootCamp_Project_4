# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8841fe90-e7c3-4b06-b622-3c6945b0f0fb",
# META       "default_lakehouse_name": "Processed_data",
# META       "default_lakehouse_workspace_id": "e6c63ae4-2a13-432b-88da-942625116c8a",
# META       "known_lakehouses": [
# META         {
# META           "id": "8841fe90-e7c3-4b06-b622-3c6945b0f0fb"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime

# Initial metadata record
metadata_data = [
    Row(pipeline_name="customers_pipeline", source_table="customers_schema_v1", last_watermark=datetime(1900, 1, 1, 0, 0, 0)),
    Row(pipeline_name="orders_pipeline", source_table="orders", last_watermark=datetime(1900, 1, 1, 0, 0, 0)),
    Row(pipeline_name="transactions_pipeline", source_table="transactions", last_watermark=datetime(1900, 1, 1, 0, 0, 0))
]

# Schema
metadata_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("last_ingestion_time", TimestampType(), True)
])

# Create dataframe
metadata_df = spark.createDataFrame(
    metadata_data,
    metadata_schema
)

# Save metadata table
metadata_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("metadata_control")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
