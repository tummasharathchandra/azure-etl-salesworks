from pyspark.sql.functions import *
from pyspark.sql.types import *

# Use Unity Catalog and Silver schema
spark.sql("USE CATALOG <your_catalog>")
spark.sql("USE SCHEMA silver_schema")

# Read Bronze tables
df_product = spark.table("your_catalog.bronze_schema.Product")
df_region = spark.table("your_catalog.bronze_schema.Region")
df_reseller = spark.table("your_catalog.bronze_schema.Reseller")
df_sales = spark.table("<your_catalog>.bronze_schema.Sales")
df_salesperson = spark.table("<your_catalog>.bronze_schema.Salesperson")
df_salespersonregion = spark.table("<your_catalog>.bronze_schema.SalespersonRegion")
df_targets = spark.table("<your_catalog>.bronze_schema.Targets")

# --------------------
# Data Cleaning Logic
# --------------------

# Product: drop unused columns and nulls
df_product = df_product.drop("Background_Color_Format", "Font_Color_Format")
df_product = df_product.na.drop()

# Reseller: rename columns
df_reseller = df_reseller.withColumnRenamed("State_Province", "Province") \
                         .withColumnRenamed("Country_Region", "Country")

# Sales: fix date type
df_sales = df_sales.withColumn("OrderDate", to_date("OrderDate"))

# Targets: fix date type and drop nulls
df_targets = df_targets.withColumn("TargetMonth", to_date("TargetMonth"))
df_targets = df_targets.na.drop()

# --------------------
# Write to Silver (Unity Catalog)
# --------------------

def write_to_silver_table(df, table_name):
    df.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(f"<your_catalog>.silver_schema.{table_name}")

write_to_silver_table(df_product, "Product_Silver")
write_to_silver_table(df_region, "Region_Silver")
write_to_silver_table(df_reseller, "Reseller_Silver")
write_to_silver_table(df_sales, "Sales_Silver")
write_to_silver_table(df_salesperson, "Salesperson_Silver")
write_to_silver_table(df_salespersonregion, "SalespersonRegion_Silver")
write_to_silver_table(df_targets, "Targets_Silver")

# --------------------
# Also write to ADLS Gen2 (Silver container)
# --------------------

silver_base_path = "abfss://silver@<your_storage_account>.dfs.core.windows.net"

df_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{silver_base_path}/product_silver")

df_region.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{silver_base_path}/region_silver")

df_reseller.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{silver_base_path}/reseller_silver")

df_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{silver_base_path}/sales_silver")

df_salesperson.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{silver_base_path}/salesperson_silver")

df_salespersonregion.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{silver_base_path}/salespersonregion_silver")

df_targets.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{silver_base_path}/targets_silver")

# Quick validation
spark.sql("SELECT * FROM <your_catalog>.silver_schema.product_silver LIMIT 100").show()
