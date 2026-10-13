# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NYC Taxi → Fabric Lakehouse → Import Semantic Model (TMDL)
# 
# ### Important:  
# **TMDL defines the source and storage mode, but it does not contain reusable user credentials. You must do a full refresh and provide Power BI/Fabric cloud connection and credentials.**
# 
# 1. Creates or reuses a Lakehouse in the current workspace.
# 2. Downloads public NYC TLC Yellow Taxi data and taxi-zone lookup data.
# 3. Creates Delta tables: `FactTrip`, `DimDate`, `DimLocation`.
# 4. Reads the Lakehouse SQL analytics endpoint details.
# 5. Generates a semantic-model definition in TMDL.
# 6. Creates a semantic model with **Import** partitions.
# 


# CELL ********************

MAX_FACT_ROWS = 20000
RECREATE_SEMANTIC_MODEL = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

LAKEHOUSE_NAME = "NYCTaxi_Lakehouse"
SEMANTIC_MODEL_NAME = "NYCTaxi_Import"

TAXI_PARQUET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2024-01.parquet"
)

ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/"
    "taxi_zone_lookup.csv"
)



print("Configuration loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import requests
import base64
import json
import uuid
from pathlib import Path
from pyspark.sql import functions as F

import notebookutils
import sempy.fabric as fabric

workspace_id = fabric.get_workspace_id()
print("Workspace ID:", workspace_id)

existing = {
    x.displayName: x
    for x in notebookutils.lakehouse.list(workspaceId=workspace_id)
}

if LAKEHOUSE_NAME in existing:
    lakehouse = existing[LAKEHOUSE_NAME]
    print(f"Reusing Lakehouse: {LAKEHOUSE_NAME} ({lakehouse.id})")
else:
    lakehouse = notebookutils.lakehouse.create(
        LAKEHOUSE_NAME,
        "NYC Taxi lab Lakehouse created from a Fabric notebook.",
        workspaceId=workspace_id,
    )
    print(f"Created Lakehouse: {LAKEHOUSE_NAME} ({lakehouse.id})")

lakehouse_id = lakehouse.id

lakehouse_props = notebookutils.lakehouse.getWithProperties(
    LAKEHOUSE_NAME,
    workspaceId=workspace_id,
)

print("Lakehouse ID:", lakehouse_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tmp_dir = Path("/tmp/nyc_taxi_fabric_lab")
tmp_dir.mkdir(parents=True, exist_ok=True)

taxi_file = tmp_dir / "yellow_tripdata_2024-01.parquet"
zone_file = tmp_dir / "taxi_zone_lookup.csv"

def download(url, target):
    if target.exists() and target.stat().st_size > 0:
        print(f"Already downloaded: {target}")
        return

    print(f"Downloading {url}")
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(target, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    print(f"Saved {target} ({target.stat().st_size:,} bytes)")

download(TAXI_PARQUET_URL, taxi_file)
download(ZONE_LOOKUP_URL, zone_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

raw = spark.read.parquet(f"file:{taxi_file}")

if MAX_FACT_ROWS is not None:
    raw = raw.limit(MAX_FACT_ROWS)

fact = (
    raw
    .filter(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("PULocationID").isNotNull()
        & F.col("DOLocationID").isNotNull()
    )
    .select(
        F.col("VendorID").cast("long").alias("VendorID"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("PickupDateTime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("DropoffDateTime"),
        F.date_format(F.col("tpep_pickup_datetime"), "yyyyMMdd").cast("long").alias("PickupDateKey"),
        F.col("PULocationID").cast("long").alias("PickupLocationID"),
        F.col("DOLocationID").cast("long").alias("DropoffLocationID"),
        F.col("passenger_count").cast("double").alias("PassengerCount"),
        F.col("trip_distance").cast("double").alias("TripDistance"),
        F.col("payment_type").cast("long").alias("PaymentType"),
        F.col("fare_amount").cast("double").alias("FareAmount"),
        F.col("tip_amount").cast("double").alias("TipAmount"),
        F.col("total_amount").cast("double").alias("TotalAmount"),
    )
)

bounds = fact.agg(
    F.min(F.to_date("PickupDateTime")).alias("MinDate"),
    F.max(F.to_date("PickupDateTime")).alias("MaxDate"),
).first()

min_date = bounds["MinDate"]
max_date = bounds["MaxDate"]

dim_date = (
    spark.sql(
        f'''
        SELECT explode(
            sequence(
                to_date('{min_date}'),
                to_date('{max_date}'),
                interval 1 day
            )
        ) AS Date
        '''
    )
    .select(
        F.date_format("Date", "yyyyMMdd").cast("long").alias("DateKey"),
        F.col("Date"),
        F.year("Date").cast("long").alias("Year"),
        F.month("Date").cast("long").alias("MonthNumber"),
        F.date_format("Date", "MMMM").alias("MonthName"),
        F.quarter("Date").cast("long").alias("Quarter"),
        F.dayofmonth("Date").cast("long").alias("DayOfMonth"),
    )
)

dim_location = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"file:{zone_file}")
    .select(
        F.col("LocationID").cast("long").alias("LocationID"),
        F.col("Borough").cast("string").alias("Borough"),
        F.col("Zone").cast("string").alias("Zone"),
        F.col("service_zone").cast("string").alias("ServiceZone"),
    )
)

print("FactTrip rows:", fact.count())
print("DimDate rows:", dim_date.count())
print("DimLocation rows:", dim_location.count())



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_https = lakehouse_props.properties["oneLakeTablesPath"]

prefix = "https://onelake.dfs.fabric.microsoft.com/"
assert tables_https.startswith(prefix), tables_https

relative = tables_https[len(prefix):]
ws_part, rest = relative.split("/", 1)
tables_abfs = f"abfss://{ws_part}@onelake.dfs.fabric.microsoft.com/{rest}"

print("Writing to:", tables_abfs)

(
    fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{tables_abfs}/FactTrip")
)

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{tables_abfs}/DimDate")
)

(
    dim_location.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{tables_abfs}/DimLocation")
)

print("Delta tables written.")

time.sleep(20)

print("Lakehouse tables:")
for t in notebookutils.lakehouse.listTables(
    LAKEHOUSE_NAME,
    workspaceId=workspace_id,
):
    print(" -", t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deadline = time.time() + 300

while True:
    lakehouse_props = notebookutils.lakehouse.getWithProperties(
        LAKEHOUSE_NAME,
        workspaceId=workspace_id,
    )

    sql_props = lakehouse_props.properties["sqlEndpointProperties"]
    status = sql_props["provisioningStatus"]
    print("SQL endpoint status:", status)

    if status == "Success":
        break

    if status == "Failed":
        raise RuntimeError("Lakehouse SQL analytics endpoint provisioning failed.")

    if time.time() >= deadline:
        raise TimeoutError("SQL analytics endpoint was not ready within 5 minutes.")

    time.sleep(10)

sql_server = sql_props["connectionString"]
sql_database = sql_props["id"]

print("SQL server:", sql_server)
print("SQL database / endpoint ID:", sql_database)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def m_escape(value):
    return value.replace('"', '""')

server_m = m_escape(sql_server)
database_m = m_escape(sql_database)

database_tmdl = f"""database {uuid.uuid4()}
\tcompatibilityLevel: 1702
\tcompatibilityMode: powerBI
\tlanguage: 1033
"""

model_tmdl = """model Model
\tculture: en-US
\tdefaultPowerBIDataSourceVersion: powerBI_V3
\tsourceQueryCulture: en-US

ref table FactTrip
ref table DimDate
ref table DimLocation
"""

fact_tmdl = f"""table FactTrip

\tmeasure 'Trip Count' = COUNTROWS(FactTrip)
\t\tformatString: #,##0

\tmeasure 'Total Fare' = SUM(FactTrip[FareAmount])
\t\tformatString: $#,##0.00

\tmeasure 'Total Tips' = SUM(FactTrip[TipAmount])
\t\tformatString: $#,##0.00

\tmeasure 'Total Revenue' = SUM(FactTrip[TotalAmount])
\t\tformatString: $#,##0.00

\tmeasure 'Average Trip Distance' = AVERAGE(FactTrip[TripDistance])
\t\tformatString: #,##0.00

\tcolumn VendorID
\t\tdataType: int64
\t\tisHidden
\t\tsummarizeBy: none
\t\tsourceColumn: VendorID

\tcolumn PickupDateTime
\t\tdataType: dateTime
\t\tsummarizeBy: none
\t\tsourceColumn: PickupDateTime

\tcolumn DropoffDateTime
\t\tdataType: dateTime
\t\tsummarizeBy: none
\t\tsourceColumn: DropoffDateTime

\tcolumn PickupDateKey
\t\tdataType: int64
\t\tisHidden
\t\tsummarizeBy: none
\t\tsourceColumn: PickupDateKey

\tcolumn PickupLocationID
\t\tdataType: int64
\t\tisHidden
\t\tsummarizeBy: none
\t\tsourceColumn: PickupLocationID

\tcolumn DropoffLocationID
\t\tdataType: int64
\t\tsummarizeBy: none
\t\tsourceColumn: DropoffLocationID

\tcolumn PassengerCount
\t\tdataType: double
\t\tsummarizeBy: sum
\t\tsourceColumn: PassengerCount

\tcolumn TripDistance
\t\tdataType: double
\t\tsummarizeBy: sum
\t\tsourceColumn: TripDistance

\tcolumn PaymentType
\t\tdataType: int64
\t\tsummarizeBy: none
\t\tsourceColumn: PaymentType

\tcolumn FareAmount
\t\tdataType: double
\t\tsummarizeBy: sum
\t\tsourceColumn: FareAmount

\tcolumn TipAmount
\t\tdataType: double
\t\tsummarizeBy: sum
\t\tsourceColumn: TipAmount

\tcolumn TotalAmount
\t\tdataType: double
\t\tsummarizeBy: sum
\t\tsourceColumn: TotalAmount

\tpartition FactTrip = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = Sql.Database("{server_m}", "{database_m}"),
\t\t\t\tData = Source{{[Schema="dbo", Item="FactTrip"]}}[Data]
\t\t\tin
\t\t\t\tData
"""

date_tmdl = f"""table DimDate
\tdataCategory: Time

\tcolumn DateKey
\t\tdataType: int64
\t\tisKey
\t\tisHidden
\t\tsummarizeBy: none
\t\tsourceColumn: DateKey

\tcolumn Date
\t\tdataType: dateTime
\t\tformatString: yyyy-mm-dd
\t\tsummarizeBy: none
\t\tsourceColumn: Date

\tcolumn Year
\t\tdataType: int64
\t\tsummarizeBy: none
\t\tsourceColumn: Year

\tcolumn MonthNumber
\t\tdataType: int64
\t\tisHidden
\t\tsummarizeBy: none
\t\tsourceColumn: MonthNumber

\tcolumn MonthName
\t\tdataType: string
\t\tsortByColumn: MonthNumber
\t\tsummarizeBy: none
\t\tsourceColumn: MonthName

\tcolumn Quarter
\t\tdataType: int64
\t\tsummarizeBy: none
\t\tsourceColumn: Quarter

\tcolumn DayOfMonth
\t\tdataType: int64
\t\tsummarizeBy: none
\t\tsourceColumn: DayOfMonth

\tpartition DimDate = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = Sql.Database("{server_m}", "{database_m}"),
\t\t\t\tData = Source{{[Schema="dbo", Item="DimDate"]}}[Data]
\t\t\tin
\t\t\t\tData
"""

location_tmdl = f"""table DimLocation

\tcolumn LocationID
\t\tdataType: int64
\t\tisKey
\t\tsummarizeBy: none
\t\tsourceColumn: LocationID

\tcolumn Borough
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: Borough

\tcolumn Zone
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: Zone

\tcolumn ServiceZone
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: ServiceZone

\tpartition DimLocation = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = Sql.Database("{server_m}", "{database_m}"),
\t\t\t\tData = Source{{[Schema="dbo", Item="DimLocation"]}}[Data]
\t\t\tin
\t\t\t\tData
"""

relationships_tmdl = f"""relationship {uuid.uuid4()}
\tfromColumn: FactTrip.PickupDateKey
\ttoColumn: DimDate.DateKey

relationship {uuid.uuid4()}
\tfromColumn: FactTrip.PickupLocationID
\ttoColumn: DimLocation.LocationID
"""

definition_pbism = json.dumps(
    {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
        "version": "5.0",
        "settings": {"qnaEnabled": False},
    },
    indent=2,
)

tmdl_parts = {
    "definition.pbism": definition_pbism,
    "definition/database.tmdl": database_tmdl,
    "definition/model.tmdl": model_tmdl,
    "definition/tables/FactTrip.tmdl": fact_tmdl,
    "definition/tables/DimDate.tmdl": date_tmdl,
    "definition/tables/DimLocation.tmdl": location_tmdl,
    "definition/relationships.tmdl": relationships_tmdl,
}

for path, content in tmdl_parts.items():
    print("\n" + "=" * 80)
    print(path)
    print("=" * 80)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FABRIC_API = "https://api.fabric.microsoft.com/v1"

token = notebookutils.credentials.getToken("pbi")
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

def b64(text):
    return base64.b64encode(text.encode("utf-8")).decode("ascii")

def request_json(method, url, **kwargs):
    r = requests.request(method, url, headers=headers, timeout=180, **kwargs)

    if r.status_code == 202:
        location = r.headers.get("Location")
        if not location:
            r.raise_for_status()

        while True:
            retry_after = int(r.headers.get("Retry-After", "5"))
            time.sleep(min(max(retry_after, 1), 30))

            p = requests.get(location, headers=headers, timeout=180)

            if p.status_code == 202:
                r = p
                continue

            p.raise_for_status()
            body = p.json() if p.content else {}

            if body.get("status") in ("Running", "NotStarted"):
                r = p
                continue

            return body

    r.raise_for_status()
    return r.json() if r.content else {}

models = request_json(
    "GET",
    f"{FABRIC_API}/workspaces/{workspace_id}/semanticModels",
).get("value", [])

same_name = [m for m in models if m["displayName"] == SEMANTIC_MODEL_NAME]

if same_name and RECREATE_SEMANTIC_MODEL:
    for m in same_name:
        print("Deleting existing semantic model:", m["id"])
        request_json(
            "DELETE",
            f"{FABRIC_API}/workspaces/{workspace_id}/semanticModels/{m['id']}?hardDelete=true",
        )
    same_name = []

if same_name:
    semantic_model_id = same_name[0]["id"]
    print("Reusing semantic model:", semantic_model_id)
else:
    payload = {
        "displayName": SEMANTIC_MODEL_NAME,
        "description": "Import-mode NYC Taxi semantic model generated from TMDL in a Fabric notebook.",
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": b64(content),
                    "payloadType": "InlineBase64",
                }
                for path, content in tmdl_parts.items()
            ]
        },
    }

    result = request_json(
        "POST",
        f"{FABRIC_API}/workspaces/{workspace_id}/semanticModels",
        json=payload,
    )

    if result.get("id"):
        semantic_model_id = result["id"]
    else:
        time.sleep(3)
        models = request_json(
            "GET",
            f"{FABRIC_API}/workspaces/{workspace_id}/semanticModels",
        ).get("value", [])

        match = [m for m in models if m["displayName"] == SEMANTIC_MODEL_NAME]

        if not match:
            raise RuntimeError(
                "Semantic model creation completed but the item could not be resolved."
            )

        semantic_model_id = match[0]["id"]

print("Semantic model ID:", semantic_model_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
