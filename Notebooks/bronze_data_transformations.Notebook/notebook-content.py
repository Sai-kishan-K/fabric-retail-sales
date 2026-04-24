# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "edadcf78-c799-4040-8110-ead5cd654302",
# META       "default_lakehouse_name": "RetailLakehouse",
# META       "default_lakehouse_workspace_id": "4758c609-9a96-4407-9882-6214031134f5",
# META       "known_lakehouses": [
# META         {
# META           "id": "edadcf78-c799-4040-8110-ead5cd654302"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
df = spark.read.table("bronze_online_retail_raw")

df.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_renamed = (
    df
    .withColumnRenamed("_c0","invoice_no")
    .withColumnRenamed("_c1","stock_code")
    .withColumnRenamed("_c2","description")
    .withColumnRenamed("_c3","quantity")
    .withColumnRenamed("_c4","invoice_date_raw")
    .withColumnRenamed("_c5","unit_price_raw")
    .withColumnRenamed("_c6","customer_id")
    .withColumnRenamed("_c7","country")
)

bronze_renamed.printSchema()
bronze_renamed.show(10,truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Row count & Null checks

# CELL ********************

print("Total row count:", bronze_renamed.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when, count

null_summary = bronze_renamed.select([
    count(when(col(c).isNull() | (col(c) == ""), c)).alias(c)
    for c in bronze_renamed.columns
])

null_summary.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_renamed.select("country").distinct().orderBy("country").show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_renamed.select("country").distinct().orderBy("country").show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check price formating

# CELL ********************

bronze_renamed.select("unit_price_raw").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_renamed.select(
    F.regexp_replace("unit_price_raw", ",", ".").alias("unit_price_fixed")
).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_renamed.select("invoice_date_raw").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for cancellation invoice

# CELL ********************

bronze_renamed.filter(F.col("invoice_no").startswith("C")).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Cancellation invoice count:", bronze_renamed.filter(F.col("invoice_no").startswith("C")).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_typed_preview = (
    bronze_renamed
    .withColumn("quantity_int", F.col("quantity").cast("int"))
    .withColumn("unit_price_double", F.regexp_replace("unit_price_raw", ",", ".").cast("double"))
    .withColumn("invoice_ts", F.to_timestamp("invoice_date_raw", "dd/MM/yyyy HH:mm"))
)

bronze_typed_preview.select(
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "quantity_int",
    "unit_price_raw",
    "unit_price_double",
    "invoice_date_raw",
    "invoice_ts",
    "customer_id",
    "country"
).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check parse sucess

# CELL ********************

bronze_typed_preview.select([
    count(when(F.col(c).isNull(), c)).alias(c)
    for c in ["quantity_int", "unit_price_double", "invoice_ts"]
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_renamed.write.mode("overwrite").format("delta").saveAsTable("bronze_retail_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
