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
# META         },
# META         {
# META           "id": "41b4058f-572e-48a4-b3ac-2b8fa1982c0e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Silver to Gold Lakhouse transformations

# CELL ********************

from pyspark.sql import functions as F

silver_sales_df = spark.table("silver_sales_clean")
silver_returns_df = spark.table("silver_returns")

print("silver_sales_clean:", silver_sales_df.count())
print("silver_returns:", silver_returns_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Build gold fact tables
gold_fact_sales = silver_sales_df.select(
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

gold_fact_returns = silver_returns_df.select(
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

#Build gold dimension
gold_dim_product = (
    silver_sales_df
    .select("stock_code", "description")
    .filter(
        F.col("stock_code").isNotNull() &
        F.col("description").isNotNull()
    )
    .dropDuplicates(["stock_code", "description"])
)

gold_dim_customer = (
    silver_sales_df
    .select("customer_id", "country")
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id", "country"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Build daily sales aggregate
gold_sales_daily = (
    silver_sales_df
    .groupBy("invoice_date")
    .agg(
        F.sum("sales_amount").alias("total_revenue"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum("quantity").alias("total_quantity")
    )
    .orderBy("invoice_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_sales_monthly = (
    silver_sales_df
    .groupBy("invoice_year", "invoice_month")
    .agg(
        F.sum("sales_amount").alias("total_revenue"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum("quantity").alias("total_quantity")
    )
    .orderBy("invoice_year", "invoice_month")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_country_performance = (
    silver_sales_df
    .groupBy("country")
    .agg(
        F.sum("sales_amount").alias("total_revenue"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum("quantity").alias("total_quantity")
    )
    .orderBy(F.col("total_revenue").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_fact_sales.show(5, truncate=False)
gold_fact_returns.show(5, truncate=False)
gold_dim_product.show(5, truncate=False)
gold_dim_customer.show(5, truncate=False)
gold_sales_daily.show(5, truncate=False)
gold_sales_monthly.show(5, truncate=False)
gold_country_performance.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_base_path = "abfss://RetailSales_Returns@onelake.dfs.fabric.microsoft.com/Retail_Gold_Lakehouse.Lakehouse/Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Load the Tabls to the lakehouse

# CELL ********************

gold_fact_sales.write.mode("overwrite").format("delta").save(f"{gold_base_path}/dbo/gold_fact_sales")
gold_fact_returns.write.mode("overwrite").format("delta").save(f"{gold_base_path}/dbo/gold_fact_returns")
gold_dim_product.write.mode("overwrite").format("delta").save(f"{gold_base_path}/dbo/gold_dim_product")
gold_dim_customer.write.mode("overwrite").format("delta").save(f"{gold_base_path}/dbo/gold_dim_customer")
gold_sales_daily.write.mode("overwrite").format("delta").save(f"{gold_base_path}/dbo/gold_sales_daily")
gold_sales_monthly.write.mode("overwrite").format("delta").save(f"{gold_base_path}/dbo/gold_sales_monthly")
gold_country_performance.write.mode("overwrite").format("delta").save(f"{gold_base_path}/dbo/gold_country_performance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
