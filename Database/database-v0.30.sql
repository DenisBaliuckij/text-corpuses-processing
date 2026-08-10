USE [TextCorpuses]
GO

-- Fixes a lock convoy under pdf_downloading's 64-way concurrency (2026-08-10
-- incident): PDFUrl LIKE '%pattern%' forces a table scan (leading wildcard
-- defeats indexing), so concurrent UPDATE TOP(1) callers took overlapping
-- U-locks while scanning ~378k+ rows, and PdfSourceRotation's single-row
-- read-then-update serialized every caller behind whichever transaction was
-- currently scanning. At 64 concurrent workers this produced a 60+-session
-- blocking chain and pinned sqlservr at 200%+ CPU, dragging host load high
-- enough to break SSH/FTP entirely. Adds ROWLOCK/READPAST/UPDLOCK hints to
-- both claim UPDATEs so a worker skips rows another worker is currently
-- examining instead of blocking on them - no change to selection logic or
-- results, only locking behavior.
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
		('%gutenberg_science%'),
		('%gutenberg_social_science%'),
		('%gutenberg_law%'),
		('%gutenberg_history%'),
		('%gutenberg_philosophy_religion%'),
		('%gutenberg_poetry_drama%'),
		('%gutenberg_children%'),
		('%gutenberg_literature%'),
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

		UPDATE TOP(1) dbo.PdfDocuments WITH (ROWLOCK, READPAST, UPDLOCK)
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
		UPDATE TOP(1) dbo.PdfDocuments WITH (ROWLOCK, READPAST, UPDLOCK)
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
