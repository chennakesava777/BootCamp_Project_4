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

metadata_df = spark.table("metadata_control")

last_watermark = metadata_df.filter(
    metadata_df.pipeline_name == "customers_pipeline"
).select("last_ingestion_time").collect()[0][0]

print(last_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# """datetime_add('microsecond', 1, todatetime('{last_watermark}'))"""

kustoQuery = f"""
customers_schema_v1
| extend ingestion_time = ingestion_time()
| where ingestion_time > datetime({last_watermark})
| project
    Customer_id,
    Name,
    Email,
    City,
    Created_at,
    ingestion_time
"""
# The query URI for reading the data e.g. https://<>.kusto.data.microsoft.com.
kustoUri = "https://trd-gtnvz20723g8dh3w9s.z5.kusto.fabric.microsoft.com"
# The database with data to be read.
database = "StreamLakehouse"
# The access credentials.
accessToken = mssparkutils.credentials.getToken(kustoUri)
customer_Kustodf  = spark.read\
    .format("com.microsoft.kusto.spark.synapse.datasource")\
    .option("accessToken", accessToken)\
    .option("kustoCluster", kustoUri)\
    .option("kustoDatabase", database)\
    .option("kustoQuery", kustoQuery).load()

# Example that uses the result data frame.
display(customer_Kustodf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_Kustodf.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_df = customer_Kustodf.select(
    "Customer_id",
    "Name",
    "Email",
    "City",
    "Created_at",
    "ingestion_time"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

clean_df = customer_df \
    .dropDuplicates(["Customer_id"]) \
    .withColumn("Name", trim(col("Name"))) \
    .withColumn("Email", lower(trim(col("Email")))) \
    .withColumn("City", initcap(trim(col("City")))) \
    .withColumn("Created_at",to_timestamp("Created_at"))\
    .withColumn("ingestion_time",to_timestamp("Created_at")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("Customer_id")

final_df = clean_df.withColumn(
    "customer_sk",
    row_number().over(window_spec) + 1000
)

final_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = final_df.select(
    "customer_sk",
    "Customer_id",
    "Name",
    "Email",
    "City",
    "Created_at",
    "ingestion_time"
)

final_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.write \
    .format("delta") \
    .option("mergeSchema",True)\
    .mode("append") \
    .saveAsTable("dim_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import max

# new watermark value
new_watermark = customer_Kustodf.agg(
    max("ingestion_time")
).collect()[0][0]

print(new_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql import SparkSession

# target metadata table
meta_table = DeltaTable.forName(spark, "metadata_control")


# pipeline you want to update
pipeline_name = "customers_pipeline"


# create a small source dataframe for merge
source_df = spark.createDataFrame([
    Row(
        pipeline_name=pipeline_name,
        last_ingestion_time=new_watermark
    )
])

# MERGE (update only matching pipeline row)
meta_table.alias("t").merge(
    source_df.alias("s"),
    "t.pipeline_name = s.pipeline_name"
).whenMatchedUpdate(set={
    "last_ingestion_time": "s.last_ingestion_time"
}).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
