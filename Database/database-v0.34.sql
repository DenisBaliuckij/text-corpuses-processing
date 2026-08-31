USE [TextCorpuses]
GO

-- Adds a stored procedure to register an already-local PDF (produced by langembed's
-- document-normalization bridge, not downloaded by any of the existing download-*
-- DAGs) directly into the LaTeX conversion queue. AddPdfUrl (earlier migration)
-- inserts with LocationInFileSystem='' for the *download* queue -- it does not set
-- NeedsLatexConversion and assumes the file still needs fetching. This proc is for
-- the opposite case: the file already exists on the FTP server (uploaded by
-- dags/sciparse_bridge.py) and only needs LaTeX conversion.
--
-- Note: the filtered index that required SET QUOTED_IDENTIFIER ON for every write to
-- PdfDocuments (database-v0.32.sql) was already replaced with a plain index in
-- database-v0.33.sql, so no such requirement applies here -- SET QUOTED_IDENTIFIER ON
-- is kept only for consistency with this file's sibling stored procedures.
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[RegisterPdfForLatexConversion]
    @pdfUrl nvarchar(max),
    @locationInFileSystem nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS(SELECT * FROM dbo.PdfDocuments WHERE LocationInFileSystem = @locationInFileSystem)
    BEGIN
        INSERT INTO dbo.PdfDocuments (PDFUrl, LocationInFileSystem, NeedsLatexConversion, InsertedAt)
        VALUES (@pdfUrl, @locationInFileSystem, 1, SYSUTCDATETIME())
    END
END
GO
