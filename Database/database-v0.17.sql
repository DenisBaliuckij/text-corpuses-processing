USE [TextCorpuses]
GO

-- Adds custom-query support: an authenticated web UI lets a user pick a
-- source and a structured query (matching search_configs.json's per-source
-- criterion shape) and get matching PDFs collected into a folder they name.
-- CustomQuery is the job row (one per submitted query, modeled on
-- GraphConstructionJob); CustomQueryPdf tracks each discovered PDF's
-- reuse-or-download outcome (modeled on GraphConstructionFiles).
CREATE TABLE dbo.CustomQuery (
	ID int IDENTITY(1,1) NOT NULL,
	SourceName nvarchar(50) NOT NULL,
	CriterionJson nvarchar(max) NOT NULL,
	FolderName nvarchar(200) NOT NULL,
	Status int NOT NULL,
	Error nvarchar(max) NULL,
	CreatedAt datetime NOT NULL,
	LastStatusChangeAt datetime NOT NULL,
	CONSTRAINT [PK_CustomQuery] PRIMARY KEY CLUSTERED ([ID] ASC)
)
GO

CREATE TABLE dbo.CustomQueryPdf (
	ID int IDENTITY(1,1) NOT NULL,
	CustomQueryId int NOT NULL,
	PDFUrl nvarchar(max) NOT NULL,
	Status int NOT NULL,
	DestinationPath nvarchar(max) NULL,
	CONSTRAINT [PK_CustomQueryPdf] PRIMARY KEY CLUSTERED ([ID] ASC),
	CONSTRAINT [FK_CustomQueryPdf_CustomQuery] FOREIGN KEY (CustomQueryId) REFERENCES dbo.CustomQuery(ID)
)
GO

CREATE INDEX IX_CustomQueryPdf_CustomQueryId ON dbo.CustomQueryPdf(CustomQueryId)
GO

CREATE PROCEDURE [dbo].[CreateCustomQuery]
	@sourceName nvarchar(50),
	@criterionJson nvarchar(max),
	@folderName nvarchar(200)
AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO dbo.CustomQuery (SourceName, CriterionJson, FolderName, Status, CreatedAt, LastStatusChangeAt)
	VALUES (@sourceName, @criterionJson, @folderName, 0, GETUTCDATE(), GETUTCDATE());
	SELECT SCOPE_IDENTITY() AS ID;
END
GO

CREATE PROCEDURE [dbo].[GetPendingCustomQueries]
AS
BEGIN
	SET NOCOUNT ON;
	SELECT ID, SourceName, CriterionJson, FolderName, Status
	FROM dbo.CustomQuery
	WHERE Status IN (0, 20);
END
GO

CREATE PROCEDURE [dbo].[AddCustomQueryPdf]
	@customQueryId int,
	@pdfUrl nvarchar(max)
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @id int = (SELECT ID FROM dbo.CustomQueryPdf WHERE CustomQueryId = @customQueryId AND PDFUrl = @pdfUrl);
	IF @id IS NULL
	BEGIN
		INSERT INTO dbo.CustomQueryPdf (CustomQueryId, PDFUrl, Status)
		VALUES (@customQueryId, @pdfUrl, 0);
		SET @id = SCOPE_IDENTITY();
	END
	SELECT @id AS ID;
END
GO

-- Reuse-detection: matches regardless of which fragment tag (if any) the
-- URL was originally discovered/downloaded under, so a PDF already fetched
-- by e.g. the standard arxiv pipeline is found and reused by a later custom
-- query for the same underlying URL.
CREATE PROCEDURE [dbo].[FindExistingDownload]
	@baseUrl nvarchar(max)
AS
BEGIN
	SET NOCOUNT ON;
	SELECT TOP 1 LocationInFileSystem
	FROM dbo.PdfDocuments
	WHERE (PDFUrl = @baseUrl OR PDFUrl LIKE @baseUrl + '#%')
	  AND LocationInFileSystem NOT IN ('', 'NA');
END
GO

CREATE PROCEDURE [dbo].[MarkCustomQueryPdfStatus]
	@id int,
	@status int,
	@destinationPath nvarchar(max) = NULL
AS
BEGIN
	SET NOCOUNT ON;
	UPDATE dbo.CustomQueryPdf
	SET Status = @status, DestinationPath = @destinationPath
	WHERE ID = @id;
END
GO

CREATE PROCEDURE [dbo].[UpdateCustomQueryStatus]
	@id int,
	@status int,
	@error nvarchar(max) = NULL
AS
BEGIN
	SET NOCOUNT ON;
	UPDATE dbo.CustomQuery
	SET Status = @status, Error = @error, LastStatusChangeAt = GETUTCDATE()
	WHERE ID = @id;
END
GO

CREATE PROCEDURE [dbo].[GetCustomQueryStatus]
	@id int
AS
BEGIN
	SET NOCOUNT ON;
	SELECT ID, SourceName, CriterionJson, FolderName, Status, Error, CreatedAt, LastStatusChangeAt
	FROM dbo.CustomQuery
	WHERE ID = @id;
END
GO

CREATE PROCEDURE [dbo].[GetCustomQueryPdfs]
	@customQueryId int
AS
BEGIN
	SET NOCOUNT ON;
	SELECT ID, PDFUrl, Status, DestinationPath
	FROM dbo.CustomQueryPdf
	WHERE CustomQueryId = @customQueryId;
END
GO

CREATE PROCEDURE [dbo].[GetRecentCustomQueries]
AS
BEGIN
	SET NOCOUNT ON;
	SELECT TOP 50 ID, SourceName, FolderName, Status, CreatedAt
	FROM dbo.CustomQuery
	ORDER BY ID DESC;
END
GO

-- Rows still pending fulfillment via the shared pdf_downloading queue:
-- their tagged URL (PDFUrl + '#customquery_' + id + '_' + slug) now shows
-- up in PdfDocuments once downloaded, at which point they're promoted to
-- Status=20 (downloaded) by custom_query_processor.
CREATE PROCEDURE [dbo].[GetPendingCustomQueryDownloads]
	@customQueryId int
AS
BEGIN
	SET NOCOUNT ON;
	SELECT ID, PDFUrl
	FROM dbo.CustomQueryPdf
	WHERE CustomQueryId = @customQueryId AND Status = 0;
END
GO
