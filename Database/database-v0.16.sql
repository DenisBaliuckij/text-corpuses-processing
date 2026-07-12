USE [TextCorpuses]
GO

-- Adds fair round-robin ordering across sources to GetPdfToDownload.
-- The atomic-claim fix in v0.15 stopped concurrent workers from
-- redundantly claiming the same row, but the query still had no
-- ordering across sources - it claimed whatever row the underlying
-- scan reached first, which in practice meant exhausting arxiv's
-- entire backlog before ever reaching cyberleninka, pubmed, or any
-- Gujarati source. This table tracks which source was served last, so
-- each call advances to the next source in rotation (skipping any with
-- nothing pending) instead of draining one source completely before
-- touching another.
CREATE TABLE dbo.PdfSourceRotation (
	LastIndex int NOT NULL
)
GO

INSERT INTO dbo.PdfSourceRotation (LastIndex) VALUES (0)
GO

ALTER PROCEDURE [dbo].[GetPdfToDownload]
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @claimThreshold datetime2 = DATEADD(MINUTE, -5, SYSUTCDATETIME());
	DECLARE @result TABLE (PDFUrl nvarchar(max));

	-- Canonical source list. Order fixes each source's rotation slot;
	-- adding a new source later just appends another row here.
	DECLARE @sources TABLE (Idx int IDENTITY(0,1), Pattern nvarchar(50));
	INSERT INTO @sources (Pattern) VALUES
		('%gujarati_literature%'),
		('%gujarati_news%'),
		('%gujarati_science_natural%'),
		('%gujarati_science_social%'),
		('%arxiv%'),
		('%lenin%'),
		('%ncbi%'),
		('%semanticscholar%');

	DECLARE @numSources int = (SELECT COUNT(*) FROM @sources);
	DECLARE @startIdx int = (SELECT LastIndex FROM dbo.PdfSourceRotation);
	DECLARE @i int = 0;
	DECLARE @pattern nvarchar(50);
	DECLARE @tryIdx int;
	DECLARE @claimedIdx int = NULL;

	WHILE @i < @numSources AND NOT EXISTS (SELECT 1 FROM @result)
	BEGIN
		SET @tryIdx = (@startIdx + 1 + @i) % @numSources;
		SELECT @pattern = Pattern FROM @sources WHERE Idx = @tryIdx;

		UPDATE TOP(1) dbo.PdfDocuments
		SET ClaimedAt = SYSUTCDATETIME()
		OUTPUT INSERTED.PDFUrl INTO @result
		WHERE LocationInFileSystem = ''
		  AND PDFUrl NOT LIKE '%springer%'
		  AND PDFUrl LIKE @pattern
		  AND (ClaimedAt IS NULL OR ClaimedAt <= @claimThreshold);

		IF EXISTS (SELECT 1 FROM @result)
			SET @claimedIdx = @tryIdx;

		SET @i = @i + 1;
	END

	-- Fallback: nothing matched any known-source pattern (e.g. a URL that
	-- doesn't fit any explicit bucket) - claim unrestricted so that pool
	-- isn't permanently starved either.
	IF NOT EXISTS (SELECT 1 FROM @result)
	BEGIN
		UPDATE TOP(1) dbo.PdfDocuments
		SET ClaimedAt = SYSUTCDATETIME()
		OUTPUT INSERTED.PDFUrl INTO @result
		WHERE LocationInFileSystem = ''
		  AND PDFUrl NOT LIKE '%springer%'
		  AND (ClaimedAt IS NULL OR ClaimedAt <= @claimThreshold);
	END

	IF @claimedIdx IS NOT NULL
		UPDATE dbo.PdfSourceRotation SET LastIndex = @claimedIdx;

	SELECT PDFUrl FROM @result;
END
GO
