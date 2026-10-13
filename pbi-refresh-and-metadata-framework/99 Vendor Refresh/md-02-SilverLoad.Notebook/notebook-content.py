# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "422dd95b-e22e-46aa-8fc3-62313e32922b",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "a943fa08-b40a-41d0-b0fc-2aa283a4393c",
# META       "known_lakehouses": [
# META         {
# META           "id": "422dd95b-e22e-46aa-8fc3-62313e32922b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # md-02-SilverLoad
# 
# Default lakehouse: **Bronze**.
# 
# The vendor-partitioned version of 02-SilverLoad. For each row in the SilverTable control table where `Load = 1`, it reads the Bronze file, runs the table and column transformations, and saves a Silver table. Tables listed in `IncrementalRefreshControl` are read from their per-vendor files and partitioned by `VendorID` in Silver.
# 
# `VendorID` is optional:
# 
# - **Blank:** every table is processed. Partitioned tables read every vendor's file (`<name>_Vendor*.parquet`) and are saved partitioned by `VendorID`.
# - **Set:** only the partitioned tables are processed. Only that vendor's file is read, and the save replaces only that vendor's partition (Delta `replaceWhere`). Other vendors are left as they are.

# PARAMETERS CELL ********************

UDF_ITEM_NAME = "SilverFunctions"
VendorID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import pandas as pd

BATCH_SIZE = 200_000   # rows per function call, keeps each call under the 30 MB response limit

functions = notebookutils.udf.getFunctions(UDF_ITEM_NAME)

silver_tables = spark.sql("SELECT * FROM Metadata.SilverTable WHERE Load = 1").collect()
silver_columns = spark.sql("SELECT * FROM Metadata.SilverColumn").collect()
partitioned_tables = {row.TableName for row in spark.sql("SELECT DISTINCT TableName FROM Metadata.IncrementalRefreshControl").collect()}

if VendorID:
    silver_tables = [table for table in silver_tables if table.DestinationTableName in partitioned_tables]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in silver_tables:

    # Read the Bronze file. Partitioned tables have one file per vendor: read them all, or just this vendor's.
    file_name = table.SourceFileName
    if table.DestinationTableName in partitioned_tables:
        name, extension = file_name.rsplit(".", 1)
        file_name = f"{name}_Vendor{VendorID or '*'}.{extension}"
    file_format = file_name.split(".")[-1]
    df = spark.read.format(file_format).option("multiLine", True).load(f"Files/{file_name}").toPandas()

    # Table transformation
    if table.Transformation is not None:
        df = getattr(functions, table.Transformation)(df)

    # Column transformations, sent to the function in batches
    for column in silver_columns:
        if column.TableName == table.DestinationTableName:
            rule = {column.ColumnName: {"rule": column.Transformation, **json.loads(column.Parameters)}}
            batches = [
                functions.clean_table(df[[column.ColumnName]][start:start + BATCH_SIZE], rule)
                for start in range(0, len(df), BATCH_SIZE)
            ]
            df[column.ColumnName] = pd.concat(batches)[column.ColumnName].to_numpy()

    # Save to Silver
    writer = spark.createDataFrame(df).write.mode("overwrite")
    if VendorID:
        writer = writer.option("replaceWhere", f"VendorID = {VendorID}")    # replace this vendor's partition only
    elif table.DestinationTableName in partitioned_tables:
        writer = writer.option("overwriteSchema", "true").partitionBy("VendorID")
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(f"Silver.{table.DestinationTableName}")

    print("Loaded", table.DestinationTableName, len(df), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
