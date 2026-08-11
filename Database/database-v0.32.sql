USE [TextCorpuses]
GO

-- Replaces GetPDFLocationForLatexConvertation's unindexable LEFT JOIN scan
-- (both LocationInFileSystem and PDFLocation are nvarchar(max), which SQL
-- Server cannot index directly - exceeds the 900-byte key limit) with a
-- maintained status flag, mirroring the ClaimedAt pattern GetPdfToDownload
-- already uses successfully (database-v0.15.sql). The old query's
-- "LocationInFileSystem IS NOT NULL" filter was also a no-op in practice -
-- confirmed live, the column is never NULL (only '', 'NA', or a real path) -
-- so it was silently attempting "conversion" on undownloaded/excluded rows
-- too; the new flag only ever gets set for rows that were genuinely
-- downloaded, closing that as a side effect. This also closes a pre-existing
-- weak-claiming race (the old design could theoretically hand the same PDF
-- to two concurrent callers, since a claimed-but-unresolved row still
-- matched the selection filter).
ALTER TABLE dbo.PdfDocuments ADD NeedsLatexConversion bit NOT NULL DEFAULT 0
GO

ALTER TABLE dbo.PdfDocuments ADD LatexClaimedAt datetime2 NULL
GO

CREATE INDEX IX_PdfDocuments_NeedsLatexConversion
    ON dbo.PdfDocuments(NeedsLatexConversion, LatexClaimedAt)
    WHERE NeedsLatexConversion = 1
GO

-- One-time backfill: flag every already-downloaded row that doesn't yet
-- have a resolved (non-empty) LatexDocuments entry.
UPDATE dbo.PdfDocuments
SET NeedsLatexConversion = 1
WHERE LocationInFileSystem NOT IN ('', 'NA')
  AND NOT EXISTS (
    SELECT 1 FROM dbo.LatexDocuments latex
    WHERE latex.PDFLocation = PdfDocuments.LocationInFileSystem
      AND latex.LatexLocation <> ''
  )
GO

ALTER PROCEDURE [dbo].[GetPDFLocationForLatexConvertation]
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @claimThreshold datetime2 = DATEADD(MINUTE, -30, SYSUTCDATETIME());
	DECLARE @result TABLE (PDFUrl nvarchar(max));
	DECLARE @pdfUrl nvarchar(max);

	UPDATE TOP(1) dbo.PdfDocuments WITH (ROWLOCK, READPAST, UPDLOCK)
	SET LatexClaimedAt = SYSUTCDATETIME()
	OUTPUT INSERTED.LocationInFileSystem INTO @result
	WHERE NeedsLatexConversion = 1
	  AND (LatexClaimedAt IS NULL OR LatexClaimedAt <= @claimThreshold);

	SELECT @pdfUrl = PDFUrl FROM @result;

	IF @pdfUrl IS NOT NULL AND NOT EXISTS(SELECT * FROM dbo.LatexDocuments WHERE PDFLocation = @pdfUrl)
	BEGIN
		INSERT INTO dbo.LatexDocuments(PDFLocation, LatexLocation)
		VALUES(@pdfUrl, '')
	END

	SELECT @pdfUrl
END
GO

ALTER PROCEDURE [dbo].[SaveLatexDocumentLocation]
	@pdfUrl nvarchar(max),
	@latexLocation nvarchar(max)
AS
BEGIN
	SET NOCOUNT ON;

	UPDATE dbo.LatexDocuments
	SET LatexLocation = @latexLocation
	WHERE PdfLocation = @pdfUrl;

	UPDATE dbo.PdfDocuments
	SET NeedsLatexConversion = 0
	WHERE LocationInFileSystem = @pdfUrl;
END
GO

ALTER PROCEDURE [dbo].[SavePdfFileLocation]
	@pdfUrl nvarchar(max),
	@fileLocation nvarchar(max)
AS
BEGIN
	SET NOCOUNT ON;

	UPDATE dbo.PdfDocuments
	SET LocationInFileSystem = @fileLocation,
	    NeedsLatexConversion = CASE WHEN @fileLocation NOT IN ('', 'NA') THEN 1 ELSE 0 END
	WHERE PdfUrl = @pdfUrl;
END
GO
