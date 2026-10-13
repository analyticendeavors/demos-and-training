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

# # GoldTransform_FactTrip
# 
# Keep the trip columns business users need and give them friendly names. Silver stores the pickup and dropoff values as text, because the cleaning function returns text, with the time already removed, so they're cast to dates here.
# 
# 03-GoldLoad runs this notebook and gets the Spark SQL back as its exit value, then runs it against Silver and saves the result to the Gold table `FactTrip`.

# CELL ********************

sql = """
SELECT
    VendorID                           AS `Vendor ID`,
    CAST(PickupDateTime AS DATE)       AS `Pickup Date`,
    CAST(DropoffDateTime AS DATE)      AS `Dropoff Date`,
    PickupLocationID                   AS `Pickup Location ID`,
    DropoffLocationID                  AS `Dropoff Location ID`,
    PaymentType                        AS `Payment Type`,
    FareAmount                         AS `Fare Amount`,
    TotalAmount                        AS `Total Amount`
FROM FactTrip
"""

notebookutils.notebook.exit(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
