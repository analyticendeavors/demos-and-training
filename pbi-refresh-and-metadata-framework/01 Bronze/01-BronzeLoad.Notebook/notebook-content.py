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

# # 01-BronzeLoad
# 
# Default lakehouse: **Metadata**.
# 
# Reads the Bronze control table and runs the pipeline named on each row where `Load = 1`, passing `SourceTableName` and `ConnectionID` as pipeline parameters. Each run finishes before the next starts. Everything lands in the Bronze lakehouse as a file:
# 
# - **LoadSQL** copies `dbo.<SourceTableName>` to `Files/<SourceTableName>.parquet`.
# - **LoadJSON** copies `Files/<SourceTableName>.json` to `Files/<SourceTableName>.json`.
# 
# Notebookutils can run notebooks but not pipelines, so this uses the Fabric REST client from semantic link (`sempy`), which is already authenticated in the notebook.

# CELL ********************

import time
import sempy.fabric as fabric

workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
client = fabric.FabricRestClient()

bronze_rows = spark.sql("SELECT * FROM Metadata.Bronze WHERE Load = 1").collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row in bronze_rows:
    pipeline_id = fabric.resolve_item_id(row.Pipeline, item_type="DataPipeline")

    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
        json={
            "executionData": {
                "parameters": {
                    "SourceTableName": row.SourceTableName,
                    "ConnectionID": row.ConnectionID,
                }
            }
        },
    )

    # Wait for the pipeline run to finish before starting the next one
    status = "NotStarted"
    while status in ("NotStarted", "InProgress"):
        time.sleep(15)
        status = client.get(response.headers["Location"]).json()["status"]

    print(row.SourceTableName, row.Pipeline, status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
