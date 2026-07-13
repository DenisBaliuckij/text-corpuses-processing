USE [TextCorpuses]
GO

-- Fixes a regression introduced in v0.15: AddPdfUrl's bare
-- "insert into dbo.PdfDocuments values(@url, '')" supplied only 2 values,
-- but the table has had a 3rd non-identity column (ClaimedAt) since v0.15,
-- so every call has been failing with error 213 ("Column name or number of
-- supplied values does not match table definition"). This has been silently
-- starving every source that had zero pre-existing PdfDocuments rows before
-- v0.15 shipped (semantic_scholar, gujarati_science_natural/social, and the
-- new gujarati_science_archive all showed 0 downloaded/pending despite DAG
-- runs reporting success) - sources with a pre-v0.15 backlog were unaffected
-- since their existing rows were unaffected by new inserts failing.
ALTER PROCEDURE [dbo].[AddPdfUrl]
	@url nvarchar(max)
AS
BEGIN
	SET NOCOUNT ON;

	if not exists(select * from dbo.PdfDocuments where PdfUrl = @url)
	begin
		insert into dbo.PdfDocuments (PDFUrl, LocationInFileSystem)
		values(@url, '')
	end
END
GO
