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

# # <center> md-00-Setup
# # <center> ![AE](https://images.squarespace-cdn.com/content/v1/65bd4af6ba431675a17a3276/590b6325-e7ca-433b-94a1-b0feba7383dc/Favicon.png?format=10w)
# <br>
# 
# #### Sets up the vendor-partitioned version of the framework. Run it once, after the original Orchestrator has loaded Gold, because the semantic model's columns are read from the Gold tables.
# #### It creates the `IncrementalRefreshControl` table, then builds the semantic model on the Gold lakehouse with one FactTrip partition per vendor per month.
# #### Default lakehouse: **Metadata**. The control table is saved there.
# #### Afterwards:
# 1. Open the semantic model's settings and sign in under **Data source credentials** (OAuth2). The model definition can't carry credentials.
# 2. Run the **md-VendorRefresh** pipeline once with `VendorID` left blank, to load every vendor and partition.
# 
# ---

# MARKDOWN ********************

# ## ⬇️ Parameters
# - `DATASET`: the semantic model to create. If it already exists, it is replaced.
# - `TABLE_NAME`: the table to partition by vendor, through Bronze, Silver, Gold and the model
# - `VENDOR_IDS`: the vendors to create partitions for
# - `YEAR`: the year to create monthly partitions for
# - `RELATIONSHIPS`: the model's relationships, as (from table, from column, to table, to column)

# PARAMETERS CELL ********************

DATASET = "md-NYCTaxi_Gold"
TABLE_NAME = "FactTrip"
VENDOR_IDS = [1, 2, 6]
YEAR = 2024
RELATIONSHIPS = [
    ("FactTrip", "Vendor ID", "Vendor", "Vendor ID"),
    ("FactTrip", "Payment Type", "PaymentType", "PaymentType"),
    ("FactTrip", "Pickup Location ID", "DimLocation", "LocationID"),
    ("FactTrip", "Pickup Date", "DimDate", "Date"),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔧 Create the control table
# One row per vendor per month. It drives the md- notebooks:
# 
# - md-01-BronzeLoad loads the table one vendor at a time, into one file per vendor.
# - md-02-SilverLoad partitions the Silver table by `VendorID`.
# - md-03-GoldLoad replaces one vendor's rows in Gold.
# - The model below gets one partition per row, and md-04-RefreshModel refreshes a vendor's partitions and records `LastRefresh`.

# CELL ********************

from calendar import monthrange
from datetime import date

rows = []
for vendor_id in VENDOR_IDS:
    for month in range(1, 13):
        rows.append((
            TABLE_NAME,
            f"Vendor{vendor_id}_{YEAR}_{month:02d}",
            vendor_id,
            date(YEAR, month, 1),
            date(YEAR, month, monthrange(YEAR, month)[1]),
            None,
        ))

control = spark.createDataFrame(
    rows,
    "TableName string, PartitionName string, VendorID int, MonthStart date, MonthEnd date, LastRefresh timestamp",
)

control.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("Metadata.IncrementalRefreshControl")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔧 Create the semantic model
# Builds an Import model on the Gold lakehouse's SQL analytics endpoint:
# 
# - **Tables** come from the Gold control table, and each table's columns come from its Gold schema.
# - **Partitions:** a table listed in `IncrementalRefreshControl` gets one partition per row, filtered to that vendor and month. The upper bound is the first day of the next month, so no rows fall between partitions. Every other table gets one partition.
# - **Relationships** come from `RELATIONSHIPS`.
# 
# The model is sent as one TMSL `createOrReplace` command, so re-running this cell rebuilds it. The partitions are empty until the model is refreshed.

# CELL ********************

import uuid
from datetime import timedelta
import sempy.fabric as fabric

# Gold SQL analytics endpoint
endpoint = notebookutils.lakehouse.getWithProperties("Gold").properties["sqlEndpointProperties"]
source = f'Sql.Database("{endpoint["connectionString"]}", "{endpoint["id"]}")'

# Spark type to model type
DATA_TYPES = {
    "string": "string", "long": "int64", "integer": "int64", "double": "double", "float": "double",
    "decimal": "decimal", "boolean": "boolean", "date": "dateTime", "timestamp": "dateTime",
}

gold_tables = spark.sql("SELECT DestinationTableName FROM Metadata.Gold").collect()
control_rows = spark.sql("SELECT * FROM Metadata.IncrementalRefreshControl").collect()

tables = []
for gold_table in gold_tables:
    table_name = gold_table.DestinationTableName

    columns = [
        {"name": field.name, "dataType": DATA_TYPES[field.dataType.typeName()], "sourceColumn": field.name}
        for field in spark.table(f"Gold.{table_name}").schema
    ]

    partition_rows = [row for row in control_rows if row.TableName == table_name]

    if partition_rows:
        # One partition per vendor and month
        partitions = []
        for row in partition_rows:
            start = row.MonthStart
            end = row.MonthEnd + timedelta(days=1)
            partitions.append({
                "name": row.PartitionName,
                "mode": "import",
                "source": {"type": "m", "expression": f'''
let
    Source = {source},
    Data = Source{{[Schema = "dbo", Item = "{table_name}"]}}[Data],
    FilteredRows = Table.SelectRows(
        Data,
        each [Vendor ID] = {row.VendorID}
            and [Pickup Date] >= #date({start.year}, {start.month}, {start.day})
            and [Pickup Date] < #date({end.year}, {end.month}, {end.day})
    )
in
    FilteredRows
'''.strip()},
            })
    else:
        # One partition for the whole table
        partitions = [{
            "name": table_name,
            "mode": "import",
            "source": {"type": "m", "expression": f'''
let
    Source = {source},
    Data = Source{{[Schema = "dbo", Item = "{table_name}"]}}[Data]
in
    Data
'''.strip()},
        }]

    tables.append({"name": table_name, "columns": columns, "partitions": partitions})

relationships = [
    {"name": str(uuid.uuid4()), "fromTable": from_table, "fromColumn": from_column, "toTable": to_table, "toColumn": to_column}
    for from_table, from_column, to_table, to_column in RELATIONSHIPS
]

fabric.execute_tmsl({
    "createOrReplace": {
        "object": {"database": DATASET},
        "database": {
            "name": DATASET,
            "compatibilityLevel": 1604,
            "model": {
                "culture": "en-US",
                "defaultPowerBIDataSourceVersion": "powerBI_V3",
                "tables": tables,
                "relationships": relationships,
            },
        },
    }
})

print(f"Created {DATASET}: {len(tables)} tables, {len(control_rows)} vendor partitions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
