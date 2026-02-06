from pyspark.sql.functions import *
from pyspark.sql.types import *

# Use Unity Catalog
spark.sql("USE CATALOG <name>")
spark.sql("USE SCHEMA bronze_schema")

# Base path for Bronze container
base_path = "abfss://bronze@<your_storage_account>.dfs.core.windows.net/"

# Read CSV files from ADLS Gen2
df_product           = spark.read.csv(f"{base_path}Product/", header=True, inferSchema=True)
df_region            = spark.read.csv(f"{base_path}Region/", header=True, inferSchema=True)
df_reseller          = spark.read.csv(f"{base_path}Reseller/", header=True, inferSchema=True)
df_sales             = spark.read.csv(f"{base_path}Sales/", header=True, inferSchema=True)
df_salesperson       = spark.read.csv(f"{base_path}Salesperson/", header=True, inferSchema=True)
df_salespersonregion = spark.read.csv(f"{base_path}SalespersonRegion/", header=True, inferSchema=True)
df_targets           = spark.read.csv(f"{base_path}Targets/", header=True, inferSchema=True)

# Helper function to write Delta tables to Bronze
def write_to_bronze_table(df, table_name):
    df.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(f"salesworks.bronze_schema.{table_name}")

# Write tables
write_to_bronze_table(df_product, "Product")
write_to_bronze_table(df_region, "Region")
write_to_bronze_table(df_reseller, "Reseller")
write_to_bronze_table(df_sales, "Sales")
write_to_bronze_table(df_salesperson, "Salesperson")
write_to_bronze_table(df_salespersonregion, "SalespersonRegion")
write_to_bronze_table(df_targets, "Targets")

# Quick check
display(df_product)
