# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1b35c528-75ab-4c56-9787-2f036e39dbbb",
# META       "default_lakehouse_name": "Silver",
# META       "default_lakehouse_workspace_id": "a943fa08-b40a-41d0-b0fc-2aa283a4393c",
# META       "known_lakehouses": [
# META         {
# META           "id": "1b35c528-75ab-4c56-9787-2f036e39dbbb"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # md-03-GoldLoad
# 
# Default lakehouse: **Silver**. The gold transformation notebooks have no lakehouse of their own, so they inherit it, and their Spark SQL reads the Silver tables by name.
# 
# The vendor-partitioned version of 03-GoldLoad. For each row in the Gold control table, it runs the named gold transformation notebook, which returns its Spark SQL, then runs that SQL and saves the result as a Gold table.
# 
# `VendorID` is optional:
# 
# - **Blank:** every Gold table is rebuilt.
# - **Set:** only the tables listed in `IncrementalRefreshControl` are processed. The SQL result is filtered to that vendor, and the save replaces only that vendor's rows (Delta `replaceWhere` on `Vendor ID`). Other vendors are left as they are.

# PARAMETERS CELL ********************

VendorID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")

gold_rows = spark.sql("SELECT * FROM Metadata.Gold").collect()
partitioned_tables = {row.TableName for row in spark.sql("SELECT DISTINCT TableName FROM Metadata.IncrementalRefreshControl").collect()}

if VendorID:
    gold_rows = [row for row in gold_rows if row.DestinationTableName in partitioned_tables]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row in gold_rows:
    sql = notebookutils.notebook.run(row.GoldTransformationName)
    df = spark.sql(sql)

    if VendorID:
        # Replace only this vendor's rows
        vendor_filter = f"`Vendor ID` = {VendorID}"
        df.where(vendor_filter).write.mode("overwrite").option("replaceWhere", vendor_filter).saveAsTable(f"Gold.{row.DestinationTableName}")
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Gold.{row.DestinationTableName}")

    print("Loaded", row.DestinationTableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
