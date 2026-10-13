# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_setup_logging
# 
# Creates the control objects in the **Logging** warehouse for both pipelines. Each cell runs T-SQL against Logging with the `%%tsql` magic, which works in Python notebooks.
# 
# | Object | Used by | Purpose |
# |---|---|---|
# | `dbo.PipelineMetadata` | pl_metadata_copy | Source and destination pairs. `UpdateValue` 1 = load, 0 = skip. |
# | `dbo.PipelineMetadataIncremental` | pl_metadata_copy_incremental | The same, plus `LoadType` (Full or Incremental) and the `WatermarkColumn` for incremental tables. |
# | `dbo.Watermark` | pl_metadata_copy_incremental | The highest value already loaded, per incremental table. |
# | `dbo.usp_write_watermark` | pl_metadata_copy_incremental | Saves the new watermark after a successful copy. |
# 
# Safe to re-run: the two metadata tables are rebuilt, but existing watermarks are kept, so the next incremental run doesn't reload rows that are already in Bronze.

# MARKDOWN ********************

# ## Control table: PipelineMetadata

# CELL ********************

# MAGIC %%tsql -artifact Logging -type Warehouse
# MAGIC DROP TABLE IF EXISTS dbo.PipelineMetadata;
# MAGIC 
# MAGIC CREATE TABLE dbo.PipelineMetadata
# MAGIC (
# MAGIC     SourceTable       VARCHAR(100) NOT NULL,
# MAGIC     DestinationTable  VARCHAR(100) NOT NULL,
# MAGIC     UpdateValue       BIT          NOT NULL
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO dbo.PipelineMetadata (SourceTable, DestinationTable, UpdateValue)
# MAGIC VALUES
# MAGIC     ('Date',      'dim_date',      1),
# MAGIC     ('Medallion', 'dim_medallion', 1),
# MAGIC     ('Time',      'dim_time',      1),
# MAGIC     ('Trip',      'fact_trip',     1);

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Control table: PipelineMetadataIncremental
# The dimensions are small, so they're overwritten in full. `Trip` is loaded incrementally on `DateID`.

# CELL ********************

# MAGIC %%tsql -artifact Logging -type Warehouse
# MAGIC DROP TABLE IF EXISTS dbo.PipelineMetadataIncremental;
# MAGIC 
# MAGIC CREATE TABLE dbo.PipelineMetadataIncremental
# MAGIC (
# MAGIC     SourceTable       VARCHAR(100) NOT NULL,
# MAGIC     DestinationTable  VARCHAR(100) NOT NULL,
# MAGIC     UpdateValue       BIT          NOT NULL,
# MAGIC     LoadType          VARCHAR(20)  NOT NULL,
# MAGIC     WatermarkColumn   VARCHAR(100) NULL
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO dbo.PipelineMetadataIncremental (SourceTable, DestinationTable, UpdateValue, LoadType, WatermarkColumn)
# MAGIC VALUES
# MAGIC     ('Date',      'dim_date',      1, 'Full',        NULL),
# MAGIC     ('Medallion', 'dim_medallion', 1, 'Full',        NULL),
# MAGIC     ('Time',      'dim_time',      1, 'Full',        NULL),
# MAGIC     ('Trip',      'fact_trip',     1, 'Incremental', 'DateID');

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Watermark table
# `WatermarkValue` is text so it can hold a number or a date. The pipeline compares it as a quoted literal, which SQL converts to the column's type.

# CELL ********************

# MAGIC %%tsql -artifact Logging -type Warehouse
# MAGIC IF OBJECT_ID('dbo.Watermark') IS NULL
# MAGIC     CREATE TABLE dbo.Watermark
# MAGIC     (
# MAGIC         TableName       VARCHAR(100) NOT NULL,
# MAGIC         WatermarkValue  VARCHAR(50)  NOT NULL
# MAGIC     );

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Add a starting watermark of `0` for each incremental table that doesn't have one yet, so the first run loads every row.

# CELL ********************

# MAGIC %%tsql -artifact Logging -type Warehouse
# MAGIC INSERT INTO dbo.Watermark (TableName, WatermarkValue)
# MAGIC SELECT SourceTable, '0'
# MAGIC FROM dbo.PipelineMetadataIncremental
# MAGIC WHERE LoadType = 'Incremental'
# MAGIC   AND SourceTable NOT IN (SELECT TableName FROM dbo.Watermark);

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Stored procedure: usp_write_watermark

# CELL ********************

# MAGIC %%tsql -artifact Logging -type Warehouse
# MAGIC CREATE OR ALTER PROCEDURE dbo.usp_write_watermark
# MAGIC     @TableName       VARCHAR(100),
# MAGIC     @WatermarkValue  VARCHAR(50)
# MAGIC AS
# MAGIC BEGIN
# MAGIC     UPDATE dbo.Watermark
# MAGIC     SET WatermarkValue = @WatermarkValue
# MAGIC     WHERE TableName = @TableName;
# MAGIC END

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Check

# CELL ********************

# MAGIC %%tsql -artifact Logging -type Warehouse
# MAGIC SELECT 'PipelineMetadata' AS ControlTable, COUNT(*) AS Rows FROM dbo.PipelineMetadata
# MAGIC UNION ALL
# MAGIC SELECT 'PipelineMetadataIncremental', COUNT(*) FROM dbo.PipelineMetadataIncremental
# MAGIC UNION ALL
# MAGIC SELECT 'Watermark', COUNT(*) FROM dbo.Watermark;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
