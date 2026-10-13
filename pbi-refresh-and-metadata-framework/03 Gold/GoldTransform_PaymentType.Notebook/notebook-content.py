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

# # GoldTransform_PaymentType
# 
# Select all columns from the Silver `PaymentMethod` table.
# 
# 03-GoldLoad runs this notebook and gets the Spark SQL back as its exit value, then runs it against Silver and saves the result to the Gold table `PaymentType`.

# CELL ********************

sql = """
SELECT *
FROM PaymentMethod
"""

notebookutils.notebook.exit(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
