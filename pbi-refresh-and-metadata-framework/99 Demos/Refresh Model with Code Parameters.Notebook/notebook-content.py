# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6e972b21-ccf2-46e5-8855-8db112b8e2e1",
# META       "default_lakehouse_name": "NYCTaxi_Lakehouse",
# META       "default_lakehouse_workspace_id": "a943fa08-b40a-41d0-b0fc-2aa283a4393c",
# META       "known_lakehouses": [
# META         {
# META           "id": "6e972b21-ccf2-46e5-8855-8db112b8e2e1"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## <center> Incremental Refresh Run 
# ## <center> ![AE](https://images.squarespace-cdn.com/content/v1/65bd4af6ba431675a17a3276/590b6325-e7ca-433b-94a1-b0feba7383dc/Favicon.png?format=10w) 
# <br>
# 
# ###### This notebook is designed as a supporting artifact for the Dataminds training and is for demonstration purposes only and not designed for use. 
# ###### [Supporting YouTube Video](https://youtu.be/xBJut2CO2hI)
# <br>
# 
# ###### Edit the `Initialize Variables` cell as needed. It can also take parameters from a pipeline. 
# ###### <u>**You must add a default Lakehouse via the pane on the left.**</u> All logging will be saved here in a table called `RefreshLogging`. <br> <br>
#  ---

# MARKDOWN ********************

# ## 🛠️ Install Dependencies
# Do not change this cell. These are needed for the notebook to run

# CELL ********************

import sempy.fabric as fabric 
from sempy.fabric import refresh_dataset, get_refresh_execution_details, TOMWrapper
import time
from datetime import datetime
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ⬇️ Edit this cell only
# ## Initialize Variables
# Defines input arguments provided to the notebook.  
# These align to the [Power BI Enhanced Refresh API](https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh)
# - `run_id`: This run identify. Default to YYYY-MM-DD-hh:mm.
# - `dataset` : The semantic model name (text)
# - `workspace` : The workspace name (text)
# - `incrementalTables` : A list of tables to refresh by partition. Write in this format:
#     - ["Table A", "Table B"]  
#     <br>  
# - `apply_refresh_policy` : If an incremental refresh policy is defined, determines whether to apply the policy. Modes are true or false. If the policy isn't applied, the full process leaves partition definitions unchanged, and fully refreshes all partitions in the table.
# - `max_parallelism` : For non-incremental tables, how many tables will run in parallel when refreshing
# - `refresh_type` : The type of processing to perform. Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment.


# PARAMETERS CELL ********************

run_id = datetime.now().strftime("%Y-%m-%d-%H:%M")
dataset = "NYCTaxi_Import Custom Partitions"
workspace = "Master Power BI Refresh Optimisations and Metadata-Driven Frameworks"
input_refresh = 'full'
input_table = ["FactTrip"]
input_partition = ["Vendor1_2024_01"]
apply_refresh_policy = False
max_parallelism = 15
refresh_type = "full" # dataOnly, full, Calculate, Defragment

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 📜 Create Functions
# Defines functions to run in notebook

# CELL ********************

#Refresh the table with options
def full_refresh(workspace, dataset, tables_to_refresh):

    request_id = refresh_dataset(
            workspace            = workspace,
            dataset              = dataset,
            objects              = tables_to_refresh,
            apply_refresh_policy = apply_refresh_policy,     
            refresh_type         = refresh_type, 
            max_parallelism      = max_parallelism           
        )
    print(f"started: {request_id}")
    return request_id


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Refresh a specific partition
def incremental_refresh(workspace, dataset, table, partition):
    request_id = refresh_dataset(
        workspace            = workspace,
        dataset              = dataset,
        objects              = [
            {
                "table":     table,
                "partition": partition
            }
        ],
        apply_refresh_policy = False,      
        refresh_type         = apply_refresh_policy, 
        max_parallelism      = 1
    )
    print(f"started: {table} -- {partition}")
    return request_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📜 Show Partitions


# CELL ********************

with fabric.connect_semantic_model(dataset=dataset, workspace=workspace, readonly = False) as tom:
    all_partitions = tom.all_partitions

for part in all_partitions:
    print(part.Table.Name,  part.Name, part.RefreshedTime, part.State)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔄️ Refresh

# CELL ********************

if input_refresh == 'full':
    request_id = full_refresh(workspace, dataset, None)
else:
    request_id = incremental_refresh(workspace, dataset, input_table, input_partition)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
