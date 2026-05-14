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

#Fetching last watermark value till which the data got loaded to lakehouse
metadata_df = spark.table("metadata_control")

last_watermark = metadata_df.filter(
    metadata_df.pipeline_name == "transactions_pipeline"
).select("last_ingestion_time").collect()[0][0]

print(last_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example of query for reading data from Kusto. Replace T with your <tablename>.
kustoQuery = f"""
transactions
| extend ingestion_time = ingestion_time()
| where ingestion_time > datetime({last_watermark})
| project
    transaction_id,
    order_id,
    payment_method,
    transaction_amount,
    transaction_status,
    transaction_timestamp,
    ingestion_time
"""
# The query URI for reading the data e.g. https://<>.kusto.data.microsoft.com.
kustoUri = "https://trd-gtnvz20723g8dh3w9s.z5.kusto.fabric.microsoft.com"
# The database with data to be read.
database = "StreamLakehouse"
# The access credentials.
accessToken = mssparkutils.credentials.getToken(kustoUri)
transactions_kustodf  = spark.read\
    .format("com.microsoft.kusto.spark.synapse.datasource")\
    .option("accessToken", accessToken)\
    .option("kustoCluster", kustoUri)\
    .option("kustoDatabase", database)\
    .option("kustoQuery", kustoQuery).load()

# Example that uses the result data frame.
transactions_kustodf.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transactions_kustodf.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_timestamp,
    when
)

# Clean transactions dataframe
cleaned_df = (
    transactions_kustodf

    # Remove leading/trailing spaces from string columns
    .withColumn("transaction_id", trim(col("transaction_id")))
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("payment_method", trim(col("payment_method")))
    .withColumn("transaction_status", trim(col("transaction_status")))

    # Standardize text columns
    .withColumn("payment_method", upper(col("payment_method")))
    .withColumn("transaction_status", upper(col("transaction_status")))

    # Convert timestamp string to proper timestamp datatype
    .withColumn(
        "transaction_timestamp",
        to_timestamp(
            col("transaction_timestamp"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        )
    )

    # Handle negative/null transaction amounts
    .withColumn(
        "transaction_amount",
        when(col("transaction_amount") < 0, None)
        .otherwise(col("transaction_amount"))
    )

    # Remove records with null primary keys
    .dropna(subset=["transaction_id", "order_id"])

    # Remove duplicate transactions
    .dropDuplicates(["transaction_id"])
)

# View schema
cleaned_df.printSchema()

# Preview data
cleaned_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, floor, rand

final_df = cleaned_df.withColumn(
    "order_id_SK",
    (floor(rand() * 10000) + 10001).cast("string")
)

display(final_df.limit(10))

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
    .saveAsTable("fact_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import max

# new watermark value
new_watermark = transactions_kustodf.agg(
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
pipeline_name = "transactions_pipeline"


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
