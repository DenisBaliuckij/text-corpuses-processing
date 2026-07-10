USE [TextCorpuses]
GO

-- Fix GetPDFLocationForLatexConvertation: guard against NULL @pdfUrl when the
-- conversion queue is empty (no PdfDocuments row has a non-null LocationInFileSystem
-- yet). Previously this inserted NULL into LatexDocuments.PDFLocation, which is
-- NOT NULL, causing every empty-queue poll to fail.
ALTER PROCEDURE [dbo].[GetPDFLocationForLatexConvertation]
AS
BEGIN
	SET NOCOUNT ON;

	declare @pdfUrl as nvarchar(max)
	SELECT TOP 1 @pdfUrl = LocationInFileSystem from dbo.PdfDocuments pdfs
	left join dbo.LatexDocuments latex on  pdfs.LocationInFileSystem = latex.PDFLocation
	where (latex.ID is null or latex.LatexLocation = '') and pdfs.LocationInFileSystem is not null

	if @pdfUrl is not null and not exists(select * from dbo.LatexDocuments where PDFLocation = @pdfUrl)
	begin
		insert into dbo.LatexDocuments(PDFLocation, LatexLocation)
		values(@pdfUrl, '')
	end

	select @pdfUrl
END
GO
