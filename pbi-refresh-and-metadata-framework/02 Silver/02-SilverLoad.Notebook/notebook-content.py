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

# # 02-SilverLoad
# 
# Default lakehouse: **Bronze**.
# 
# For each row in the SilverTable control table where `Load = 1`:
# 
# 1. Read the Bronze file into a DataFrame. The file extension in the control table is the format, so `.parquet` and `.json` both work.
# 2. If the row names a table transformation, run that function on the DataFrame.
# 3. Run each column transformation from the SilverColumn control table for that table. Columns are sent to the function in batches of `BATCH_SIZE` rows, because each call's response is limited to 30 MB.
# 4. Save the result as a Silver table.
# 
# Function names come from the control tables. The functions run in the user data functions item named in the parameters cell.

# PARAMETERS CELL ********************

UDF_ITEM_NAME = "SilverFunctions"

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in silver_tables:

    # Read the Bronze file
    file_format = table.SourceFileName.split(".")[-1]
    df = spark.read.format(file_format).option("multiLine", True).load(f"Files/{table.SourceFileName}").toPandas()

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
    spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Silver.{table.DestinationTableName}")

    print("Loaded", table.DestinationTableName, len(df), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
