

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[fact_babynames](
	[sid] [bigint] primary key,
	[nameSid] [bigint] NULL,
	[yearSid] [bigint] NULL,
	[locationSid] [bigint] NULL,
	[count] [int] NULL,
	[_keyHash] [nvarchar](265) NULL,
	[_valueHash] [nvarchar](256) NULL,
	[_pipeline_run_id] [nvarchar](256) NULL,
	[_processing_date] [datetime2](7) NULL
) 
GO
ALTER TABLE [dbo].[fact_babynames]
ADD CONSTRAINT UQ_fact_babynames_keyHash UNIQUE (_keyHash);
GO
CREATE TYPE [dbo].[FactBabyNamesType] AS TABLE(
    [sid] [bigint],
	[nameSid] [bigint] NULL,
	[yearSid] [bigint] NULL,
	[locationSid] [bigint] NULL,
	[count] [int] NULL,
	[_keyHash] [nvarchar](265) NULL,
	[_valueHash] [nvarchar](256) NULL,
	[_pipeline_run_id] [nvarchar](256) NULL,
	[_processing_date] [datetime2](7) NULL
)
GO

CREATE PROCEDURE spOverwriteFactBabyNames
    @FactBabyNames [dbo].[FactBabyNamesType] READONLY
AS
BEGIN
MERGE [dbo].[fact_babynames] AS target
USING @FactBabyNames AS source
ON (target.sid = source.sid)
WHEN MATCHED THEN
    UPDATE SET nameSid = source.nameSid, yearSid = source.yearSid, locationSid = source.locationSid, count = source.count, _keyHash = source._keyHash, _pipeline_run_id = source._pipeline_run_id, _processing_date = source._processing_date
WHEN NOT MATCHED THEN
    INSERT (sid, nameSid, yearSid, locationSid, count, _keyHash, _valueHash, _pipeline_run_id, _processing_date)
    VALUES (source.sid, source.nameSid, source.yearSid, source.locationSid, source.count, source._keyHash, source._valueHash, source._pipeline_run_id, source._processing_date);
END
GO