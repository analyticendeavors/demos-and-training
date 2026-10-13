# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # GoldTransform_DimDate
# 
# Select all columns from the Silver `DimDate` table.
# 
# 03-GoldLoad runs this notebook and gets the Spark SQL back as its exit value, then runs it against Silver and saves the result to the Gold table `DimDate`.

# CELL ********************

sql = """
SELECT *
FROM DimDate
"""

notebookutils.notebook.exit(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
