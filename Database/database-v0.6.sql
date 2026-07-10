USE [TextCorpuses]
GO

-- Cross-process rate limiter for arXiv requests. arXiv's API terms ask for no
-- more than one request every 3 seconds; this table + stored procedure lets
-- every DAG that talks to arxiv.org (scraper, API downloader, PDF downloader)
-- serialize against a single shared "last request" timestamp using row locking,
-- so the limit is enforced across processes, not just within one.
CREATE TABLE [dbo].[ArxivRateLimit](
	[ID] [int] NOT NULL,
	[LastRequestEpoch] [bigint] NOT NULL,
	CONSTRAINT [PK_ArxivRateLimit] PRIMARY KEY CLUSTERED ([ID] ASC)
)
GO

INSERT INTO [dbo].[ArxivRateLimit] (ID, LastRequestEpoch) VALUES (1, 0)
GO

-- Blocks the caller (via SQL Server row locking) until at least @minGapMs
-- milliseconds have passed since the last caller's request, then reserves
-- the current time as the new "last request" instant.
CREATE PROCEDURE [dbo].[ReserveArxivRequestSlot]
	@minGapMs int = 3000
AS
BEGIN
	SET NOCOUNT ON;

	declare @now bigint = DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME())
	declare @last bigint
	declare @waitMs bigint
	declare @delayTime time

	begin transaction
		select @last = LastRequestEpoch from [dbo].[ArxivRateLimit] with (UPDLOCK, HOLDLOCK) where ID = 1
		set @waitMs = @minGapMs - (@now - @last)
		-- cap the wait so a large backlog of concurrent callers can't pile up
		-- into an unbounded delay; callers simply proceed sooner and may
		-- queue again on their next request instead.
		if @waitMs > 60000 set @waitMs = 60000
		if @waitMs > 0
		begin
			set @delayTime = DATEADD(MILLISECOND, @waitMs, CAST('00:00:00' AS time))
			waitfor delay @delayTime
			set @now = DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME())
		end
		update [dbo].[ArxivRateLimit] set LastRequestEpoch = @now where ID = 1
	commit transaction
END
GO
