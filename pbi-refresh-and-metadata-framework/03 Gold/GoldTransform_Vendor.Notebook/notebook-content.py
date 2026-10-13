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

# # GoldTransform_Vendor
# 
# Join the two Silver vendor tables, keep only the columns business users need, and give them friendly names.
# 
# 03-GoldLoad runs this notebook and gets the Spark SQL back as its exit value, then runs it against Silver and saves the result to the Gold table `Vendor`.

# CELL ********************

sql = """
SELECT
    v.VendorID       AS `Vendor ID`,
    v.VendorName     AS `Vendor Name`,
    v.Email          AS `Vendor Email`,
    c.City           AS `City`,
    c.BaseFare       AS `Base Fare`,
    c.CommissionRate AS `Commission Rate`,
    c.OnboardedDate  AS `Onboarded Date`
FROM Vendor AS v
INNER JOIN VendorContract AS c
    ON v.VendorID = c.VendorID
"""

notebookutils.notebook.exit(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
