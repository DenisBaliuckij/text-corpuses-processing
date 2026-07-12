USE [TextCorpuses]
GO

-- Critical regression fix: pdf-downloading-dag.py excludes Springer URLs
-- from actual downloading (known Springer-specific issue), but does so by
-- dequeuing via GetPdfToDownload and then returning without marking the
-- row processed. GetPdfToDownload has no ordering/exclusion of its own
-- (bare "SELECT TOP 1 ... WHERE LocationInFileSystem = ''"), so once a
-- Springer URL reaches the head of that unordered scan, every single call
-- re-fetches the exact same stuck row forever - head-of-line blocking the
-- entire download pipeline, not just Springer's share of it.
--
-- This is a temporary exclusion (until the Springer-specific issue is
-- resolved) - when Springer downloading is re-enabled, revert this
-- alongside the 'if springer in url: return' check in
-- pdf-downloading-dag.py.
ALTER PROCEDURE [dbo].[GetPdfToDownload]
AS
BEGIN
	SET NOCOUNT ON;

    select top 1 PDFUrl from dbo.PdfDocuments where LocationInFileSystem = '' and PDFUrl not like '%springer%'
END
GO
