CREATE TABLE [dbo].[PipelineMetadata] (
    [SourceTable]      VARCHAR (100) NOT NULL,
    [DestinationTable] VARCHAR (100) NOT NULL,
    [UpdateValue]      BIT           NOT NULL
);


GO