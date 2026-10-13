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

# # md-01-BronzeLoad
# 
# Default lakehouse: **Metadata**.
# 
# The vendor-partitioned version of 01-BronzeLoad. It reads the Bronze control table, keeps the rows where `Load = 1`, and runs the pipeline named on each row.
# 
# Tables listed in the `IncrementalRefreshControl` table are loaded one vendor at a time. LoadSQL gets a `Filter` of `VendorID = <id>`, so its SQL query returns only that vendor's rows, and a `FileName` of `<SourceTableName>_Vendor<id>.parquet`, so each vendor lands in its own file.
# 
# `VendorID` is optional:
# 
# - **Blank:** every table is loaded, and partitioned tables are loaded for every vendor in the control table.
# - **Set:** only that vendor's file is replaced for each partitioned table. Other vendors' files and other tables are left as they are.

# PARAMETERS CELL ********************

VendorID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import sempy.fabric as fabric

workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
client = fabric.FabricRestClient()

bronze_rows = spark.sql("SELECT * FROM Metadata.Bronze WHERE Load = 1").collect()
partitions = spark.sql("SELECT DISTINCT TableName, VendorID FROM Metadata.IncrementalRefreshControl").collect()

partitioned_tables = {p.TableName for p in partitions}
vendor_ids = [VendorID] if VendorID else sorted({p.VendorID for p in partitions})

# One entry per pipeline run: partitioned tables run once per vendor, with a filter and a file per vendor
loads = []
for row in bronze_rows:
    if row.SourceTableName in partitioned_tables:
        for vendor_id in vendor_ids:
            loads.append((row, {
                "Filter": f"VendorID = {vendor_id}",
                "FileName": f"{row.SourceTableName}_Vendor{vendor_id}.parquet",
            }))
    elif not VendorID:
        loads.append((row, {}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row, vendor_parameters in loads:
    pipeline_id = fabric.resolve_item_id(row.Pipeline, item_type="DataPipeline")

    parameters = {"SourceTableName": row.SourceTableName, "ConnectionID": row.ConnectionID, **vendor_parameters}

    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
        json={"executionData": {"parameters": parameters}},
    )

    # Wait for the pipeline run to finish before starting the next one
    status = "NotStarted"
    while status in ("NotStarted", "InProgress"):
        time.sleep(15)
        status = client.get(response.headers["Location"]).json()["status"]

    print(row.SourceTableName, vendor_parameters.get("Filter", ""), row.Pipeline, status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
