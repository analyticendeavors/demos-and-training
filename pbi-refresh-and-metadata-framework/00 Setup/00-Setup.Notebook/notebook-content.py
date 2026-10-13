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

# # 00-Setup
# 
# Run this once before the load notebooks. It:
# 
# 1. Creates the **Bronze**, **Silver**, **Gold** and **Metadata** lakehouses.
# 2. Adds the source data to the NYC taxi lakehouse (`NYCTaxi_Lakehouse`): two vendor tables and a payment method JSON file.
# 3. Creates the four metadata control tables in the Metadata lakehouse.
# 
# The NYC taxi notebook must already have run, so `FactTrip`, `DimDate` and `DimLocation` exist in the source lakehouse. The connection IDs are DMNYC_SQL and DMNYC_LH. The first code cell is a parameters cell, so the Orchestrator pipeline can override these values.
# 
# All vendor names, emails and fares are synthetic.

# PARAMETERS CELL ********************

SOURCE_LAKEHOUSE = "NYCTaxi_Lakehouse"
SQL_CONNECTION_ID = "88c095c2-5f11-4dae-ade5-975d20cdca79"   # DMNYC_SQL: source SQL analytics endpoint
JSON_CONNECTION_ID = "f3ff14e2-4d12-41bc-9d0a-e28cfb99fc49"  # DMNYC_LH: source lakehouse files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create the lakehouses

# CELL ********************

existing = [lakehouse.displayName for lakehouse in notebookutils.lakehouse.list()]

for name in ["Bronze", "Silver", "Gold", "Metadata"]:
    if name not in existing:
        notebookutils.lakehouse.create(name)
        print("Created", name)

workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
onelake = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com"

source = f"{onelake}/{notebookutils.lakehouse.get(SOURCE_LAKEHOUSE).id}"
metadata = f"{onelake}/{notebookutils.lakehouse.get('Metadata').id}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Source data: vendor tables
# 
# The vendor data is split across two tables that Gold joins on `VendorID`.
# 
# - **Vendor** has messy `VendorName` and `Email` values, and exact duplicate rows for vendors 1 and 6.
# - **VendorContract** has a messy `BaseFare`.
# 
# Both tables also carry clean columns that Gold doesn't need: `CreatedBy`, `CreatedDate`, `SourceSystem`, `ContractRef` and `LastReviewedBy`.

# CELL ********************

from datetime import date

vendor = spark.createDataFrame(
    [
        (1, "  Northline Cabs ", " Ops@Northline-Cabs.example.com ", "etl_service", date(2024, 1, 5), "CRM"),
        (1, "  Northline Cabs ", " Ops@Northline-Cabs.example.com ", "etl_service", date(2024, 1, 5), "CRM"),
        (2, "Metro Meter\u00a0Co", "mailto:Billing@MetroMeter.example.com", "etl_service", date(2024, 1, 5), "CRM"),
        (3, "Five Boroughs Dispatch\u200b", "dispatch @fiveboroughs.example.com", "etl_service", date(2024, 1, 5), "CRM"),
        (4, "Hudson Fleet Services", "N/A", "etl_service", date(2024, 2, 12), "ERP"),
        (5, "Brightway Taxi Tech", "accounts.brightway.example.com", "etl_service", date(2024, 2, 12), "ERP"),
        (6, "Empire Ride Systems", "SUPPORT@EMPIRERIDE.EXAMPLE.COM", "etl_service", date(2024, 3, 1), "CRM"),
        (6, "Empire Ride Systems", "SUPPORT@EMPIRERIDE.EXAMPLE.COM", "etl_service", date(2024, 3, 1), "CRM"),
        (7, "  Liberty Route  Mobility ", "   ", "etl_service", date(2024, 3, 1), "ERP"),
    ],
    "VendorID long, VendorName string, Email string, CreatedBy string, CreatedDate date, SourceSystem string",
)

vendor_contract = spark.createDataFrame(
    [
        (1, "$3.00", "New York", date(2019, 3, 1), 0.050, "CTR-0001", "a.patel"),
        (2, "USD 3.50", "Newark", date(2020, 6, 15), 0.045, "CTR-0002", "j.moreno"),
        (3, " 3.25 ", "Brooklyn", date(2021, 1, 10), 0.055, "CTR-0003", "a.patel"),
        (4, "3.0 USD", "Manhattan", date(2018, 11, 5), 0.040, "CTR-0004", "s.kim"),
        (5, "NULL", "Queens", date(2022, 4, 20), 0.060, "CTR-0005", "j.moreno"),
        (6, "$ 3.75", "Bronx", date(2017, 9, 12), 0.050, "CTR-0006", "s.kim"),
        (7, "-", "Jersey City", date(2023, 2, 28), 0.035, "CTR-0007", "a.patel"),
    ],
    "VendorID long, BaseFare string, City string, OnboardedDate date, CommissionRate double, ContractRef string, LastReviewedBy string",
)

vendor.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{source}/Tables/Vendor")
vendor_contract.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{source}/Tables/VendorContract")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Source data: payment method JSON
# 
# `PaymentType` codes and names follow the NYC TLC data dictionary. The two flag columns use mixed text values that `to_boolean` converts. `N/A` becomes null.

# CELL ********************

import json

payment_methods = [
    {"PaymentType": 0, "Name": "Flex Fare trip", "IsElectronic": "Y",     "IsRevenue": "yes"},
    {"PaymentType": 1, "Name": "Credit card",    "IsElectronic": "TRUE",  "IsRevenue": "1"},
    {"PaymentType": 2, "Name": "Cash",           "IsElectronic": "n",     "IsRevenue": "Y"},
    {"PaymentType": 3, "Name": "No charge",      "IsElectronic": "No",    "IsRevenue": "0"},
    {"PaymentType": 4, "Name": "Dispute",        "IsElectronic": "false", "IsRevenue": "F"},
    {"PaymentType": 5, "Name": "Unknown",        "IsElectronic": "N/A",   "IsRevenue": " no "},
    {"PaymentType": 6, "Name": "Voided trip",    "IsElectronic": "f",     "IsRevenue": "false"},
]

notebookutils.fs.put(f"{source}/Files/PaymentMethod.json", json.dumps(payment_methods, indent=2), True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Control table: Bronze
# 
# One row per source. 01-BronzeLoad runs the named pipeline and passes `SourceTableName` and `ConnectionID` to it. Both pipelines write a file to the Bronze lakehouse. `Load` switches a source on (1) or off (0): only rows with `Load = 1` are loaded.

# CELL ********************

bronze_control = spark.createDataFrame(
    [
        ("Vendor",         SQL_CONNECTION_ID,  "LoadSQL",  1),
        ("VendorContract", SQL_CONNECTION_ID,  "LoadSQL",  1),
        ("PaymentMethod",  JSON_CONNECTION_ID, "LoadJSON", 1),
        ("FactTrip",       SQL_CONNECTION_ID,  "LoadSQL",  1),
        ("DimLocation",    SQL_CONNECTION_ID,  "LoadSQL",  1),
        ("DimDate",        SQL_CONNECTION_ID,  "LoadSQL",  1),
    ],
    "SourceTableName string, ConnectionID string, Pipeline string, Load int",
)

bronze_control.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{metadata}/Tables/Bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Control table: SilverTable
# 
# One row per Bronze file. `SourceFileName` includes the extension, which 02-SilverLoad uses as the read format. `Transformation` is the name of the table function to run on the DataFrame, or null if it only needs reading and saving. `Load` switches a table on (1) or off (0): only rows with `Load = 1` are processed.

# CELL ********************

silver_table_control = spark.createDataFrame(
    [
        ("Vendor.parquet",         "Vendor",         "dedupe_table", 1),
        ("VendorContract.parquet", "VendorContract", None,           1),
        ("PaymentMethod.json",     "PaymentMethod",  None,           1),
        ("FactTrip.parquet",       "FactTrip",       None,           1),
        ("DimLocation.parquet",    "DimLocation",    None,           1),
        ("DimDate.parquet",        "DimDate",        None,           1),
    ],
    "SourceFileName string, DestinationTableName string, Transformation string, Load int",
)

silver_table_control.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{metadata}/Tables/SilverTable")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Control table: SilverColumn
# 
# One row per column that needs cleaning. `Transformation` is the function name. `Parameters` holds that function's options as JSON. Only the datetime columns need one, to say which timezone the times are in.

# CELL ********************

silver_column_control = spark.createDataFrame(
    [
        ("Vendor",         "VendorName",      "clean_text",           "{}"),
        ("Vendor",         "Email",           "standardise_email",    "{}"),
        ("VendorContract", "BaseFare",        "clean_numeric",        "{}"),
        ("PaymentMethod",  "IsElectronic",    "to_boolean",           "{}"),
        ("PaymentMethod",  "IsRevenue",       "to_boolean",           "{}"),
        ("FactTrip",       "PickupDateTime",  "standardise_datetime", '{"sourceTimezone": "America/New_York"}'),
        ("FactTrip",       "DropoffDateTime", "standardise_datetime", '{"sourceTimezone": "America/New_York"}'),
    ],
    "TableName string, ColumnName string, Transformation string, Parameters string",
)

silver_column_control.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{metadata}/Tables/SilverColumn")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Control table: Gold
# 
# One row per Gold table and the notebook that holds its transformation.

# CELL ********************

gold_control = spark.createDataFrame(
    [
        ("DimDate",     "GoldTransform_DimDate"),
        ("DimLocation", "GoldTransform_DimLocation"),
        ("PaymentType", "GoldTransform_PaymentType"),
        ("Vendor",      "GoldTransform_Vendor"),
        ("FactTrip",    "GoldTransform_FactTrip"),
    ],
    "DestinationTableName string, GoldTransformationName string",
)

gold_control.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{metadata}/Tables/Gold")

print("Setup complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
