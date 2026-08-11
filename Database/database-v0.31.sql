USE [TextCorpuses]
GO

-- Fixes a second lock convoy surfaced by the 2026-08-10 incident (see
-- database-v0.30.sql): once GetPdfToDownload's convoy was fixed, this
-- procedure became the new bottleneck. Its LEFT JOIN has no supporting
-- index - both LocationInFileSystem and PDFLocation are nvarchar(max),
-- which SQL Server cannot index directly (exceeds the 900-byte key limit) -
-- so every call scans both tables. Adding READPAST doesn't make a single
-- call faster (that needs a real indexing fix - a persisted hash column or
-- a dedicated status flag on PdfDocuments, deferred as follow-up work, not
-- rushed mid-incident), but it stops concurrent pdf_conversion callers from
-- queuing up behind each other's locks while scanning/inserting, which is
-- the actual blocking-chain symptom observed tonight.
ALTER PROCEDURE [dbo].[GetPDFLocationForLatexConvertation]
AS
BEGIN
	SET NOCOUNT ON;

	declare @pdfUrl as nvarchar(max)
	SELECT TOP 1 @pdfUrl = LocationInFileSystem from dbo.PdfDocuments pdfs
	left join dbo.LatexDocuments latex WITH (READPAST) on  pdfs.LocationInFileSystem = latex.PDFLocation
	where (latex.ID is null or latex.LatexLocation = '') and pdfs.LocationInFileSystem is not null

	if @pdfUrl is not null and not exists(select * from dbo.LatexDocuments WITH (READPAST) where PDFLocation = @pdfUrl)
	begin
		insert into dbo.LatexDocuments(PDFLocation, LatexLocation)
		values(@pdfUrl, '')
	end

	select @pdfUrl
END
GO
