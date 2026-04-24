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

# MARKDOWN ********************

# ### Silver Transfomrations

# CELL ********************

from pyspark.sql import functions as F
bronze_df = spark.table("bronze_retail_data")

bronze_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = (
    bronze_df
    .withColumn("invoice_no", F.trim(F.col("invoice_no")))
    .withColumn("stock_code", F.trim(F.col("stock_code")))
    .withColumn("description", F.trim(F.col("description")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.regexp_replace(F.col("unit_price_raw"), ",", ".").cast("double"))
    .withColumn("invoice_ts", F.to_timestamp(F.col("invoice_date_raw"), "dd/MM/yyyy HH:mm"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Add derived columns
silver_df = (
    silver_df
    .withColumn("sales_amount", F.col("quantity") * F.col("unit_price"))
    .withColumn("invoice_date", F.to_date(F.col("invoice_ts")))
    .withColumn("invoice_year", F.year(F.col("invoice_ts")))
    .withColumn("invoice_month", F.month(F.col("invoice_ts")))
    .withColumn("invoice_day", F.dayofmonth(F.col("invoice_ts")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Select only required columns
silver_df = silver_df.select(
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_ts",
    "unit_price",
    "customer_id",
    "country",
    "sales_amount",
    "invoice_date",
    "invoice_year",
    "invoice_month",
    "invoice_day"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Valdiate Nulls 

# CELL ********************

from pyspark.sql.functions import col, when, count

silver_nulls = silver_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in silver_df.columns
])

silver_nulls.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Keep only Non Null rows

silver_clean_filtered = silver_df.filter(
    F.col("invoice_no").isNotNull() &
    F.col("stock_code").isNotNull() &
    F.col("description").isNotNull() &
    F.col("invoice_ts").isNotNull() &
    F.col("unit_price").isNotNull() &
    F.col("quantity").isNotNull()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_clean_filtered.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Silver row count after filtering:", silver_clean_filtered.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Save as table in Lakehouse
silver_clean_filtered.write.mode("overwrite").format("delta").saveAsTable("silver_retail_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_returns_df = silver_df.filter(
    F.col("invoice_no").startswith("C") |
    (F.col("quantity") < 0) |
    (F.col("sales_amount") < 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_returns_df.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create sales only dataframe
silver_sales_clean_df = silver_df.filter(
    (~F.col("invoice_no").startswith("C")) &
    (F.col("quantity") > 0) &
    (F.col("sales_amount") > 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_sales_clean_df.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Original Silver count:", silver_df.count())
print("Returns count:", silver_returns_df.count())
print("Sales clean count:", silver_sales_clean_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_returns_df.select(
    F.min("quantity").alias("min_quantity"),
    F.max("quantity").alias("max_quantity"),
    F.min("sales_amount").alias("min_sales_amount"),
    F.max("sales_amount").alias("max_sales_amount")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_sales_clean_df.select(
    F.min("quantity").alias("min_quantity"),
    F.max("quantity").alias("max_quantity"),
    F.min("sales_amount").alias("min_sales_amount"),
    F.max("sales_amount").alias("max_sales_amount")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_returns_df.write.mode("overwrite").format("delta").saveAsTable("silver_returns")
silver_sales_clean_df.write.mode("overwrite").format("delta").saveAsTable("silver_sales_clean")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
