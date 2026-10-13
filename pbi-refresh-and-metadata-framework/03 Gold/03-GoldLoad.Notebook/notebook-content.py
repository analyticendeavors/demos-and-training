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

# # 03-GoldLoad
# 
# Default lakehouse: **Silver**. The gold transformation notebooks have no lakehouse of their own, so they inherit it, and their Spark SQL reads the Silver tables by name.
# 
# For each row in the Gold control table, this runs the named gold transformation notebook, which returns its Spark SQL. It then runs that SQL and saves the result as a Gold table.
# 
# The Gold column names contain spaces for business users. Delta only allows that with column mapping, so it's switched on for the tables this notebook creates.

# CELL ********************

spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")

gold_rows = spark.sql("SELECT * FROM Metadata.Gold").collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row in gold_rows:
    sql = notebookutils.notebook.run(row.GoldTransformationName)

    spark.sql(sql).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Gold.{row.DestinationTableName}")

    print("Loaded", row.DestinationTableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
