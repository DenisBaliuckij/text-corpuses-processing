USE [TextCorpuses]
GO

-- Adds a true insertion timestamp to PdfDocuments so the ops report can
-- measure URLs-inserted-per-timespan precisely. Previously there was no
-- column recording when a URL was added; ClaimedAt was only a proxy for
-- "roughly around download time", not insertion time. Existing rows are
-- left NULL (their true insertion time is unknown) - only rows inserted
-- after this migration will have an accurate InsertedAt.
ALTER TABLE dbo.PdfDocuments ADD InsertedAt datetime2 NULL;
GO

ALTER PROCEDURE [dbo].[AddPdfUrl]
	@url nvarchar(max)
AS
BEGIN
	SET NOCOUNT ON;

	if not exists(select * from dbo.PdfDocuments where PdfUrl = @url)
	begin
		insert into dbo.PdfDocuments (PDFUrl, LocationInFileSystem, InsertedAt)
		values(@url, '', SYSUTCDATETIME())
	end
END
GO
