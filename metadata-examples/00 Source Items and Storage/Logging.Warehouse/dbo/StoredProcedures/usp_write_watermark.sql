CREATE   PROCEDURE dbo.usp_write_watermark
    @TableName       VARCHAR(100),
    @WatermarkValue  VARCHAR(50)
AS
BEGIN
    UPDATE dbo.Watermark
    SET WatermarkValue = @WatermarkValue
    WHERE TableName = @TableName;
END

GO