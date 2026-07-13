USE [TextCorpuses]
GO

-- Semantic Scholar's API now rejects key requests from free/personal email
-- domains, and returns 403 on every unauthenticated request too - this
-- source cannot currently produce any URLs (0 rows in PdfDocuments,
-- confirmed via task logs). download_semantic_scholar DAG has been paused;
-- drop it from GetPdfToDownload's round-robin rotation so the loop doesn't
-- keep checking a source that can never have pending rows.
ALTER PROCEDURE [dbo].[GetPdfToDownload]
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @claimThreshold datetime2 = DATEADD(MINUTE, -5, SYSUTCDATETIME());
	DECLARE @result TABLE (PDFUrl nvarchar(max));

	DECLARE @sources TABLE (Idx int IDENTITY(0,1), Pattern nvarchar(50));
	INSERT INTO @sources (Pattern) VALUES
		('%gujarati_literature%'),
		('%gujarati_news%'),
		('%gujarati_science_natural%'),
		('%gujarati_science_social%'),
		('%arxiv%'),
		('%lenin%'),
		('%ncbi%');

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
