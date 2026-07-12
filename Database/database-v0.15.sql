USE [TextCorpuses]
GO

-- Fixes a structural throughput bug: GetPdfToDownload was a bare
-- "SELECT TOP 1 ... WHERE LocationInFileSystem = ''" with no claim/
-- reservation mechanism. Confirmed live: 8 concurrent EXEC calls all
-- returned the exact same URL, meaning every one of pdf_downloading's
-- concurrent workers redundantly re-downloads whatever row currently
-- sits at the head of the (unordered) scan - wasting most of the
-- available concurrency on duplicate work for a single URL, and
-- starving every other source (cyberleninka, pubmed, Gujarati
-- literature/news all showed zero downloads despite tens of thousands
-- of pending URLs, while arxiv alone got through).
--
-- ClaimedAt lets GetPdfToDownload atomically claim a row the moment
-- it's selected (a single UPDATE ... OUTPUT statement, which SQL Server
-- locks properly so two concurrent callers can't claim the same row),
-- so concurrent workers each get a different URL. A 5-minute claim
-- timeout releases a row back to the pool if whatever claimed it never
-- resolved (crashed worker, or an exception that never called
-- SavePdfFileLocation) - the same head-of-line-blocking risk the
-- Springer exclusion ran into, but general-purpose instead of a
-- one-off special case.
ALTER TABLE dbo.PdfDocuments ADD ClaimedAt datetime2 NULL
GO

ALTER PROCEDURE [dbo].[GetPdfToDownload]
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @claimThreshold datetime2 = DATEADD(MINUTE, -5, SYSUTCDATETIME());
	DECLARE @result TABLE (PDFUrl nvarchar(max));

	UPDATE TOP(1) dbo.PdfDocuments
	SET ClaimedAt = SYSUTCDATETIME()
	OUTPUT INSERTED.PDFUrl INTO @result
	WHERE LocationInFileSystem = ''
	  AND PDFUrl NOT LIKE '%springer%'
	  AND (ClaimedAt IS NULL OR ClaimedAt <= @claimThreshold);

	SELECT PDFUrl FROM @result;
END
GO
