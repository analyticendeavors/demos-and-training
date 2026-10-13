# Metadata-driven pipeline examples

Two Fabric pipelines that load data from a control table instead of hardcoding each table. Change a row in the table and the pipeline changes what it loads, with no edits to the pipeline itself.

| Pipeline | What it does |
|---|---|
| Metadata Refresh | Reads `dbo.PipelineMetadata` and copies each listed table from the NYCTaxi warehouse into TaxiLakehouse, overwriting it every run. |
| Incremental Metadata Refresh | The same idea, but fact tables load incrementally. A watermark table remembers the last value loaded, so each run only appends new rows into TaxiBronze. Dimensions are still overwritten in full. |

## Data
The source is Fabric's NYC Taxi sample warehouse (`Date`, `Medallion`, `Time`, `Trip`). The control tables, watermark table and stored procedure live in the Logging warehouse.

## Folders
| Folder | Contents |
|---|---|
| 00 Setup | `nb_setup_logging`, which creates and fills the control tables |
| 00 Source Items and Storage | NYCTaxi (source), Logging (control tables), TaxiLakehouse and TaxiBronze (destinations) |
| 01 Pipelines | The two pipelines |

## How to use
1. Fork this repo, create a workspace, and connect it under **Workspace settings > Git integration**, using the `metadata-examples` folder. Then sync.
2. NYCTaxi comes across empty, because Git holds the table definitions but not the data. Create a Warehouse sample from the workspace Home page, then copy the four tables into NYCTaxi from a query in the NYCTaxi warehouse:
```sql
   INSERT INTO dbo.[Date]    SELECT * FROM [<sample warehouse>].dbo.[Date];
   INSERT INTO dbo.Medallion SELECT * FROM [<sample warehouse>].dbo.Medallion;
   INSERT INTO dbo.[Time]    SELECT * FROM [<sample warehouse>].dbo.[Time];
   INSERT INTO dbo.Trip      SELECT * FROM [<sample warehouse>].dbo.Trip;
```
3. Run `nb_setup_logging` to fill the control tables.
4. Open each pipeline and check its warehouse and lakehouse connections point at this workspace's items, then run it.

## Things to try
- Set `UpdateValue = 0` on a row in `dbo.PipelineMetadata` and rerun. That table is skipped.
- Run the incremental pipeline twice. The second run appends nothing, because there's nothing newer than the watermark.
- Add a new day of trips to `dbo.Trip` and run it again. Only those rows are appended, and the watermark moves on.
