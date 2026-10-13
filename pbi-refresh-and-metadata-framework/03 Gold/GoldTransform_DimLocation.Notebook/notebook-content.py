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

# # GoldTransform_DimLocation
# 
# Select all columns from the Silver `DimLocation` table.
# 
# 03-GoldLoad runs this notebook and gets the Spark SQL back as its exit value, then runs it against Silver and saves the result to the Gold table `DimLocation`.

# CELL ********************

sql = """
SELECT *
FROM DimLocation
"""

notebookutils.notebook.exit(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
