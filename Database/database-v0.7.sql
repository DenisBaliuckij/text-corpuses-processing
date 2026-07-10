USE [TextCorpuses]
GO

-- Fix MarkProxyAsBroken: the two deletes (child relIpProxyProxyProtocols rows,
-- then the parent IPProxy row) previously ran as separate implicit
-- transactions. Under concurrent access (multiple threads downloading PDFs
-- in parallel), a concurrent AddOrUpdateProxy call for the same proxy could
-- re-insert a relIpProxyProxyProtocols row in between the two deletes,
-- causing a foreign key violation on the IPProxy delete. Wrapping both
-- deletes in one explicit transaction with UPDLOCK/HOLDLOCK on the initial
-- lookup closes that race window.
ALTER PROCEDURE [dbo].[MarkProxyAsBroken]
	@ip nvarchar(150)
AS
BEGIN
	SET NOCOUNT ON;
	declare @id as int

	begin transaction
		select @id = ID from dbo.IPProxy with (UPDLOCK, HOLDLOCK) where IP = @ip

		if @id is not null
		begin
			delete from relIpProxyProxyProtocols where IPProxyID = @id
			delete from dbo.IPProxy where ID = @id
		end
	commit transaction
END
GO
