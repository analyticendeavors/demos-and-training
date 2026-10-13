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
# META       "default_warehouse": "079fa9bc-7b76-4e1d-90cb-c40c7ae7ba23",
# META       "known_warehouses": [
# META         {
# META           "id": "079fa9bc-7b76-4e1d-90cb-c40c7ae7ba23",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # <center> Refresh Partitions by Vendor
# # <center> ![AE](https://images.squarespace-cdn.com/content/v1/65bd4af6ba431675a17a3276/590b6325-e7ca-433b-94a1-b0feba7383dc/Favicon.png?format=10w)
# <br>
# 
# #### Splits the `FactTrip` table of a semantic model into one partition per vendor per month, then refreshes one vendor's partitions in a single request.
# #### 1. **Create the control table**: one row per vendor per month, saved as `IncrementalRefreshControl`.
# #### 2. **Create the partitions**: replaces `FactTrip`'s partitions with one M partition per control table row, filtered to that vendor and month.
# #### 3. **Refresh one vendor**: refreshes every partition for `VendorID`, and only those.
# #### <u>**Attach NYCTaxi_Lakehouse as the default lakehouse**</u> in the pane on the left. The control table is saved there, and the partitions read `FactTrip` from its SQL analytics endpoint.
# 
# ---

# MARKDOWN ********************

# ## 🛠️ Install dependencies
# Semantic Link Labs provides the Tabular Object Model (TOM) connection used to create the partitions.

# CELL ********************

%pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Imports

# CELL ********************

from calendar import monthrange
from datetime import date, timedelta
import pandas as pd


from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType
from sempy.fabric import refresh_dataset
from sempy_labs.tom import connect_semantic_model
from sempy.fabric import get_refresh_execution_details, refresh_dataset


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ⬇️ Variables
# Edit this cell only. It's a parameters cell, so a pipeline can also pass these values in.
# - `dataset`: the semantic model name
# - `workspace`: the workspace that holds the semantic model
# - `fact_table`: the table to partition
# - `VENDOR_IDS`: the vendors to create partitions for
# - `YEAR`: the year to create monthly partitions for
# - `VendorID`: the vendor to refresh in the last step

# PARAMETERS CELL ********************

dataset = "NYCTaxi_Import Custom Partitions"
workspace = "Master Power BI Refresh Optimisations and Metadata-Driven Frameworks"
fact_table = "FactTrip"
VENDOR_IDS = [1, 2, 6]
YEAR = 2024
VendorID = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔧 Create the control table
# One row per vendor per month. `MonthEnd` is the last day of the month, and `LastRefresh` starts empty.

# CELL ********************

rows = []
for vendor_id in VENDOR_IDS:
    for month in range(1, 13):
        rows.append((
            f"Vendor{vendor_id}_{YEAR}_{month:02d}",
            vendor_id,
            date(YEAR, month, 1),
            date(YEAR, month, monthrange(YEAR, month)[1]),
            None,
        ))

schema = StructType([
    StructField("PartitionName", StringType(), False),
    StructField("VendorID", IntegerType(), False),
    StructField("MonthStart", DateType(), False),
    StructField("MonthEnd", DateType(), False),
    StructField("LastRefresh", TimestampType(), True),
])

spark.createDataFrame(rows, schema=schema).write.format("delta").mode("overwrite").saveAsTable("IncrementalRefreshControl")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ❔❔ Testing Queries

# CELL ********************

#Show Partitions
rows = []
with connect_semantic_model(dataset=dataset, workspace=workspace, readonly=True) as tom:
    for table in tom.model.Tables:
        for partition in table.Partitions:
            rows.append({
                "Table": table.Name,
                "Partition": partition.Name,
                "State": str(partition.State),
                "Last refreshed": str(partition.RefreshedTime),
                "Query": getattr(partition.Source, "Expression", None),
            })

display(pd.DataFrame(rows))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔧 Create the partitions
# Removes every existing partition from `fact_table`, then adds one per control table row. Each partition's M query reads `FactTrip` from the lakehouse SQL analytics endpoint and keeps one vendor and one month. The upper bound is the first day of the next month, so January covers `>= 2024-01-01` and `< 2024-02-01`, and no rows fall between partitions.
# 
# The new partitions are empty until they're refreshed.

# CELL ********************

sql_endpoint = notebookutils.lakehouse.getWithProperties("NYCTaxi_Lakehouse").properties["sqlEndpointProperties"]
control_rows = spark.table("IncrementalRefreshControl").collect()

with connect_semantic_model(dataset=dataset, workspace=workspace, readonly=False) as tom:

    # Remove the existing partitions
    table = tom.model.Tables[fact_table]
    for partition_name in [p.Name for p in table.Partitions]:
        tom.remove_object(object=table.Partitions[partition_name])

    # Add one partition per vendor and month
    for row in control_rows:
        month_start = row["MonthStart"]
        next_month_start = row["MonthEnd"] + timedelta(days=1)

        m_expression = f'''
let
    Source = Sql.Database("{sql_endpoint['connectionString']}", "{sql_endpoint['id']}"),
    FactTrip = Source{{[ Schema="dbo", Item="FactTrip"]}}[Data],
    FilteredRows = Table.SelectRows(
        FactTrip,
        each [VendorID] = {row["VendorID"]}
            and [PickupDateTime] >= #datetime({month_start.year}, {month_start.month}, {month_start.day}, 0, 0, 0)
            and [PickupDateTime] < #datetime({next_month_start.year}, {next_month_start.month}, {next_month_start.day}, 0, 0, 0)
    )
in
    FilteredRows
'''.strip()

        tom.add_m_partition(table_name=fact_table, partition_name=row["PartitionName"], expression=m_expression)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔄 Refresh one vendor
# Sends one refresh request containing every partition for `VendorID`, in month order. Refresh policy is off, so only these partitions are processed.

# CELL ********************

#Get Partitions 
partition_rows = (
    spark.table("IncrementalRefreshControl")
    .filter(f"VendorID = {VendorID}")
    .orderBy("MonthStart")
    .select("PartitionName")
    .collect()
)

partitions_to_refresh = [{"table": fact_table, "partition": row["PartitionName"]} for row in partition_rows]

print(f"-- VendorID: {VendorID}")
print("-- Partitions to refresh:")
for p in partitions_to_refresh:
    print("    ", p["partition"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Send Refresh
request_id = refresh_dataset(
    workspace=workspace,
    dataset=dataset,
    objects=partitions_to_refresh,
    apply_refresh_policy=False,
    refresh_type="full",
    max_parallelism=1,
)


print("Request ID:", request_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check Refresh Status
details = get_refresh_execution_details(dataset=dataset, refresh_request_id=request_id, workspace=workspace)
print(details.extended_status)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
