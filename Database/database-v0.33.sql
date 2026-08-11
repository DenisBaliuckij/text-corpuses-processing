USE [TextCorpuses]
GO

-- Emergency fix, ~9.5 hours after database-v0.32.sql went live: the filtered
-- index it created (IX_PdfDocuments_NeedsLatexConversion) requires
-- SET QUOTED_IDENTIFIER ON for ANY write to PdfDocuments, not just the
-- CREATE INDEX statement itself. pyodbc's default connection (used by every
-- dags/repositories/*.py class) doesn't set this, so every UPDATE from
-- Python started failing with Msg 1934 the moment the index went live -
-- pdf_conversion crash-looped outright, pdf_downloading silently caught the
-- same error somewhere and reported "success, claimed 0" every 15 minutes
-- all night, which is why the pipeline looked idle rather than errored.
-- Dropping the filtered index and recreating as a plain (non-filtered)
-- index removes the SET-option requirement entirely for all clients - loses
-- some of the size benefit (indexes the full table, not just the ~184k
-- pending rows) but is still far cheaper than the original nvarchar(max)
-- join scan, and needs no application code changes.
DROP INDEX IX_PdfDocuments_NeedsLatexConversion ON dbo.PdfDocuments
GO

CREATE INDEX IX_PdfDocuments_NeedsLatexConversion
    ON dbo.PdfDocuments(NeedsLatexConversion, LatexClaimedAt)
GO
