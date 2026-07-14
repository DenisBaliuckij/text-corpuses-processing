USE [TextCorpuses]
GO

-- Restores arxiv/cyberleninka/pubmed's round-robin share. Since v0.22 they
-- were diluted to 1-of-21 rotation slots (down from a much larger share
-- when only a handful of sources existed), which combined with the bigger
-- real backlog from the Gujarati/Russian/English expansion collapsed
-- overall pdf_downloading throughput. Give each of the three original
-- high-volume sources 3 rotation slots instead of 1.
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
		('%gujarati_law%'),
		('%gujarati_official%'),
		('%gujarati_dictionary%'),
		('%russian_science%'),
		('%russian_literature_modern%'),
		('%russian_literature_classic%'),
		('%russian_news%'),
		('%russian_law%'),
		('%russian_social_science%'),
		('%english_science%'),
		('%english_literature_modern%'),
		('%english_literature_classic%'),
		('%english_news%'),
		('%english_law%'),
		('%english_social_science%'),
		('%arxiv%'),
		('%arxiv%'),
		('%arxiv%'),
		('%lenin%'),
		('%lenin%'),
		('%lenin%'),
		('%ncbi%'),
		('%ncbi%'),
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
