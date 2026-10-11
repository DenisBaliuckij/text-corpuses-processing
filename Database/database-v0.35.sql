USE [TextCorpuses]
GO

-- 2026-10-11: add the multilingual science sources (dags/scienceDownloader.py,
-- download_science_ia / download_science_gutenberg) to GetPdfToDownload's rotation.
-- QUOTED_IDENTIFIER must be ON when (re)creating this procedure (filtered index on PdfDocuments).
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Fix for lock-convoy under 64-way concurrency (2026-08-10 incident).
-- Adds ROWLOCK/READPAST/UPDLOCK hints to both claim UPDATEs so a worker skips
-- rows currently being examined by another concurrent worker instead of
-- blocking on them. No change to selection logic/results, only locking
-- behavior. Same source-pattern rotation logic as before, unchanged.
--
-- Re-created 2026-08-19: the 2026-08-10 ALTER was run with QUOTED_IDENTIFIER
-- OFF, which baked that setting into the compiled proc. dbo.PdfDocuments has
-- a filtered index (IX_PdfDocuments_Unclaimed, WHERE LocationInFileSystem='')
-- covering exactly the claim UPDATE's WHERE clause, and SQL Server refuses
-- any INSERT/UPDATE/DELETE against a table with a filtered index unless the
-- session has QUOTED_IDENTIFIER ON - so every claim attempt since 2026-08-10
-- failed with error 1934. This ALTER is explicitly preceded by
-- SET QUOTED_IDENTIFIER ON so it's captured correctly this time.
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
		('%ncbi%'),
		-- medical sources, added 2026-10-11 (URLs tagged '#medical_<folder>.<ext>',
		-- see dags/medicalDownloader.py); PMC weighted x3 like arxiv/lenin/ncbi
		('%#medical_pmc%'),
		('%#medical_pmc%'),
		('%#medical_pmc%'),
		('%#medical_wellcome%'),
		('%#medical_ia%'),
		('%#medical_gutenberg%'),
		('%#medical_cyberleninka%'),
		-- multilingual science sources, added 2026-10-11 (dags/scienceDownloader.py)
		('%#science_ia%'),
		('%#science_ia%'),
		('%#science_gutenberg%');

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
