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
metadata_df = spark.table("metadata_control")

last_watermark = metadata_df.filter(
    metadata_df.pipeline_name == "orders_pipeline"
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
Orders
| extend ingestion_time = ingestion_time()
| where ingestion_time > datetime({last_watermark})
| project
    order_id,
    customer_id,
    product,
    quantity,
    price,
    order_timestamp,
    ingestion_time
"""
# The query URI for reading the data e.g. https://<>.kusto.data.microsoft.com.
kustoUri = "https://trd-gtnvz20723g8dh3w9s.z5.kusto.fabric.microsoft.com"
# The database with data to be read.
database = "StreamLakehouse"
# The access credentials.
accessToken = mssparkutils.credentials.getToken(kustoUri)
orders_kustoDf  = spark.read\
    .format("com.microsoft.kusto.spark.synapse.datasource")\
    .option("accessToken", accessToken)\
    .option("kustoCluster", kustoUri)\
    .option("kustoDatabase", database)\
    .option("kustoQuery", kustoQuery).load()

# Example that uses the result data frame.
orders_kustoDf.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_kustoDf.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *



# Clean and transform data
cleaned_df = (
    orders_kustoDf
    # Remove duplicate orders
    .dropDuplicates(["order_id"])

    # Trim string columns
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product", trim(col("product")))

    # Handle null values
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())

    # Replace null quantity and price
    .fillna({
        "quantity": 0,
        "price": 0.0
    })

    # Remove invalid values
    .filter(col("quantity") > 0)
    .filter(col("price") >= 0)

    # Convert timestamp string to proper timestamp
    .withColumn(
    "order_timestamp",
    to_timestamp(
        col("order_timestamp"),
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
    )
    )

    # Add derived columns
    .withColumn("order_date", to_date(col("order_timestamp")))
    .withColumn("order_year", year(col("order_timestamp")))
    .withColumn("order_month", month(col("order_timestamp")))

    # Calculate total amount
    .withColumn("total_amount", col("quantity") * col("price"))

    # Standardize product names
    .withColumn("product", initcap(col("product")))

    # Add processing timestamp
    .withColumn("processed_time", current_timestamp())
)

# Display cleaned schema
cleaned_df.printSchema()

display(cleaned_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ====================================
# CUSTOMER SKEY STARTING FROM 1001
# ====================================

customer_window = Window.orderBy("customer_id")

# Assign same skey for same customer_id
final_df = (
    cleaned_df
    .withColumn(
        "customer_skey",
        dense_rank().over(customer_window) + 1000
    )
)

# ====================================
# ORDER SKEY FROM 10001
# ====================================

order_window = Window.orderBy("order_id")

final_df = (
    final_df
    .withColumn(
        "order_skey",
        row_number().over(order_window) + 10000
    )
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
    .saveAsTable("dim_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

from pyspark.sql.functions import max

# new watermark value
new_watermark = orders_kustoDf.agg(
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
pipeline_name = "orders_pipeline"


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
