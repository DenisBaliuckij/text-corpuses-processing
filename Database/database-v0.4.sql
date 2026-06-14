USE [TextCorpuses]
GO

-- Add ResolvedFilePath and Error columns to GraphConstructionFiles
ALTER TABLE [dbo].[GraphConstructionFiles]
ADD [ResolvedFilePath] nvarchar(1000) NULL;
GO

ALTER TABLE [dbo].[GraphConstructionFiles]
ADD [Error] nvarchar(max) NULL;
GO

-- ============================================================
-- GetFileForAnaphoraResolution
-- Picks one file at Status=0 from an active job (Status 10 or 20),
-- locks it to Status=5, returns (ID, FilePath, GraphConstructionJobId).
-- ============================================================
CREATE PROCEDURE [dbo].[GetFileForAnaphoraResolution]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @fileId AS int;

    SELECT TOP 1 @fileId = f.ID
    FROM dbo.GraphConstructionFiles f
    INNER JOIN dbo.GraphConstructionJob j ON f.GraphConstructionJobId = j.ID
    WHERE f.Status = 0
    AND j.Status IN (10, 20);

    IF @fileId IS NOT NULL
    BEGIN
        UPDATE dbo.GraphConstructionFiles
        SET Status = 5
        WHERE ID = @fileId;
    END

    -- Always SELECT so pyodbc fetchone() returns None (not raise) when no file available
    SELECT f.ID, f.FilePath, f.GraphConstructionJobId
    FROM dbo.GraphConstructionFiles f
    WHERE f.ID = @fileId;
END
GO

-- ============================================================
-- MarkFileAnaphoraDone
-- Transitions file from Status=5 to Status=10, stores resolved path.
-- ============================================================
CREATE PROCEDURE [dbo].[MarkFileAnaphoraDone]
    @fileId int,
    @resolvedFilePath nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.GraphConstructionFiles
    SET Status = 10, ResolvedFilePath = @resolvedFilePath
    WHERE ID = @fileId AND Status = 5;
END
GO

-- ============================================================
-- SetFileError
-- Sets file to Status=99 and stores error message.
-- ============================================================
CREATE PROCEDURE [dbo].[SetFileError]
    @fileId int,
    @error nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.GraphConstructionFiles
    SET Status = 99, Error = @error
    WHERE ID = @fileId;
END
GO

-- ============================================================
-- GetFileForGraphBuilding
-- Picks one file at Status=10 from an active job (Status 10 or 20),
-- locks it to Status=15, returns (ID, ResolvedFilePath, GraphConstructionJobId).
-- ============================================================
CREATE PROCEDURE [dbo].[GetFileForGraphBuilding]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @fileId AS int;

    SELECT TOP 1 @fileId = f.ID
    FROM dbo.GraphConstructionFiles f
    INNER JOIN dbo.GraphConstructionJob j ON f.GraphConstructionJobId = j.ID
    WHERE f.Status = 10
    AND j.Status IN (10, 20);

    IF @fileId IS NOT NULL
    BEGIN
        UPDATE dbo.GraphConstructionFiles
        SET Status = 15
        WHERE ID = @fileId;
    END

    -- Always SELECT so pyodbc fetchone() returns None (not raise) when no file available
    SELECT f.ID, f.ResolvedFilePath, f.GraphConstructionJobId
    FROM dbo.GraphConstructionFiles f
    WHERE f.ID = @fileId;
END
GO

-- ============================================================
-- TransitionJobToExecution
-- Moves job from Status=10 to Status=20. No-op if already 20.
-- Called by build_graph DAG on first file of each job.
-- ============================================================
CREATE PROCEDURE [dbo].[TransitionJobToExecution]
    @jobId int
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.GraphConstructionJob
    SET Status = 20, LastStatusChangeAt = GETDATE()
    WHERE ID = @jobId AND Status = 10;
END
GO

-- ============================================================
-- MarkFileGraphDone
-- Transitions file from Status=15 to Status=20.
-- ============================================================
CREATE PROCEDURE [dbo].[MarkFileGraphDone]
    @fileId int
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.GraphConstructionFiles
    SET Status = 20
    WHERE ID = @fileId AND Status = 15;
END
GO

-- ============================================================
-- FinalizeCompletedJobs
-- Finds a job at Status=20 where every file is at Status=20.
-- Sets that job to Status=30 (completed). Returns the job ID.
-- ============================================================
CREATE PROCEDURE [dbo].[FinalizeCompletedJobs]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @jobId AS int;

    SELECT TOP 1 @jobId = j.ID
    FROM dbo.GraphConstructionJob j
    WHERE j.Status = 20
    AND EXISTS (
        SELECT 1 FROM dbo.GraphConstructionFiles f
        WHERE f.GraphConstructionJobId = j.ID
    )
    AND NOT EXISTS (
        SELECT 1 FROM dbo.GraphConstructionFiles f
        WHERE f.GraphConstructionJobId = j.ID
        AND f.Status != 20
    );

    IF @jobId IS NOT NULL
    BEGIN
        UPDATE dbo.GraphConstructionJob
        SET Status = 30, LastStatusChangeAt = GETDATE()
        WHERE ID = @jobId;
    END

    -- WHERE clause returns 0 rows (fetchone → None) when no job found
    SELECT @jobId AS ID WHERE @jobId IS NOT NULL;
END
GO
