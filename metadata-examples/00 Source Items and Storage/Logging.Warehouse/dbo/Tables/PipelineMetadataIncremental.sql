CREATE TABLE [dbo].[PipelineMetadataIncremental] (
    [SourceTable]      VARCHAR (100) NOT NULL,
    [DestinationTable] VARCHAR (100) NOT NULL,
    [UpdateValue]      BIT           NOT NULL,
    [LoadType]         VARCHAR (20)  NOT NULL,
    [WatermarkColumn]  VARCHAR (100) NULL
);


GO