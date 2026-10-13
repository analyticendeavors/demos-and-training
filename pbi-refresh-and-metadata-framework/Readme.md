# Power BI refresh and metadata-driven framework

An end-to-end Fabric example: a metadata-driven medallion load (Bronze, Silver, Gold) feeding Power BI semantic models, plus demos of refreshing a model by partition from a notebook.

Control tables decide what each layer does. Bronze copies sources to files, Silver cleans them with User Data Functions named in the metadata, and Gold reshapes them with Spark SQL for reporting.

## Data
Public NYC TLC Yellow Taxi trips and the taxi zone lookup, loaded by `nyc_taxi_import` (limited to 20,000 trips by default). `00-Setup` adds two small vendor tables and a payment method JSON file with deliberately messy values, so the Silver cleaning has something to fix.

## Folders
| Folder | Contents |
|---|---|
| 00 | Setup and source: the control tables, sample data and the source lakehouse |
| 01 to 03 | Medallion layers: Bronze, Silver and Gold, each with its load notebook and lakehouse |
| 10 | Semantic models, including one with custom FactTrip partitions |
| 20 | Orchestration: the pipeline that runs Bronze, Silver and Gold in order |
| 99 | Other demos: refreshing a model by partition from code, and a version of the framework that reloads a single vendor end to end |

## How to use
1. Fork this repo, create a workspace, and connect it under **Workspace settings > Git integration**, using the `pbi-refresh-and-metadata-framework` folder. Then sync.
2. Run `nyc_taxi_import` to load the source data into NYCTaxi_Lakehouse.
3. Create two connections under **Manage connections and gateways**: one to NYCTaxi_Lakehouse's SQL analytics endpoint, and one Lakehouse connection. Put their IDs into `00-Setup` (`SQL_CONNECTION_ID`, `JSON_CONNECTION_ID`) and into the LoadSQL and LoadJSON pipelines.
4. Run `00-Setup`.
5. Open SilverFunctions and publish it.
6. Re-attach each notebook's default lakehouse, because Git keeps the original workspace's. These notebooks need one:
   - Metadata: 01-BronzeLoad, md-00-Setup, md-01-BronzeLoad, md-04-RefreshModel
   - Bronze: 02-SilverLoad, md-02-SilverLoad
   - Silver: 03-GoldLoad, md-03-GoldLoad
   - NYCTaxi_Lakehouse: the partition refresh demo
7. Run the **Orchestrator** pipeline.
8. Point the semantic models at your NYCTaxi_Lakehouse SQL endpoint and set their credentials before refreshing.

For the notebooks in `99 Demos`, change the `workspace` variable to your workspace name. For the vendor refresh, run `md-00-Setup` once after the first full load, then run `md-VendorRefresh` with a `VendorID`, or leave it blank for everything.
