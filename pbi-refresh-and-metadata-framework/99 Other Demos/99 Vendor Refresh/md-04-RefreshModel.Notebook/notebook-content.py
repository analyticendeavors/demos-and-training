# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0061aa85-c241-4c6e-8885-c81b1eca7609",
# META       "default_lakehouse_name": "Metadata",
# META       "default_lakehouse_workspace_id": "a943fa08-b40a-41d0-b0fc-2aa283a4393c",
# META       "known_lakehouses": [
# META         {
# META           "id": "0061aa85-c241-4c6e-8885-c81b1eca7609"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # <center> Refresh Semantic Model
# # <center> ![AE](https://images.squarespace-cdn.com/content/v1/65bd4af6ba431675a17a3276/590b6325-e7ca-433b-94a1-b0feba7383dc/Favicon.png?format=10w)
# <br>
# 
# #### Refreshes the semantic model at the end of a load, in one refresh request.
# #### If `VendorID` is set, only that vendor's partitions are refreshed, taken from the `IncrementalRefreshControl` table. If `VendorID` is blank, the whole model is refreshed.
# #### The notebook waits for the refresh to finish, so the pipeline's run time includes the model refresh.
# #### Default lakehouse: **Metadata**. Run md-00-Setup once before using this notebook. The model is in this notebook's workspace.
# 
# ---

# MARKDOWN ********************

# ## ⬇️ Parameters
# - `VendorID`: the vendor to refresh. Leave blank for a full refresh. The md-VendorRefresh pipeline passes this in.
# - `DATASET`: the semantic model name
# - `MAX_PARALLELISM`: how many objects the refresh processes at the same time

# PARAMETERS CELL ********************

VendorID = ""
DATASET = "md-NYCTaxi_Gold"
MAX_PARALLELISM = 15

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔄 Refresh

# CELL ********************

import time
from sempy.fabric import get_refresh_execution_details, refresh_dataset

# Choose what to refresh
if VendorID:
    partitions = spark.sql(f"SELECT TableName, PartitionName FROM Metadata.IncrementalRefreshControl WHERE VendorID = {VendorID}").collect()
    objects = [{"table": row.TableName, "partition": row.PartitionName} for row in partitions]
    print(f"Refreshing {len(objects)} partitions for vendor {VendorID}")
else:
    objects = None
    print("Refreshing the whole model")

# Start one refresh request
request_id = refresh_dataset(
    dataset=DATASET,
    objects=objects,
    refresh_type="full",
    apply_refresh_policy=False,
    max_parallelism=MAX_PARALLELISM,
)

# Wait for it to finish. The status is "Unknown" while the refresh is running.
status = "Unknown"
while status == "Unknown":
    time.sleep(15)
    status = get_refresh_execution_details(dataset=DATASET, refresh_request_id=request_id).status

print("Refresh", status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 📝 Record the refresh
# Sets `LastRefresh` in the control table for the partitions that were refreshed.

# CELL ********************

if status == "Completed":
    if VendorID:
        spark.sql(f"UPDATE Metadata.IncrementalRefreshControl SET LastRefresh = current_timestamp() WHERE VendorID = {VendorID}")
    else:
        spark.sql("UPDATE Metadata.IncrementalRefreshControl SET LastRefresh = current_timestamp()")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
