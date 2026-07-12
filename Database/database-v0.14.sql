USE [TextCorpuses]
GO

-- Fixes deadlocks (error 40001) seen frequently once 4 proxy-source DAGs
-- began running concurrently. AddOrUpdateProxy previously ran as several
-- separate auto-commit statements re-querying IPProxy multiple times with
-- no explicit locking, unlike MarkProxyAsBroken (v0.7) which already
-- serializes per-IP access via UPDLOCK/HOLDLOCK. Concurrent callers for
-- different proxies interleaving across those unprotected statements -
-- combined with reads from GetLatestProxy/GetLatestFreeProxy and deletes
-- from MarkProxyAsBroken - produced inconsistent lock acquisition and
-- genuine deadlocks.
--
-- Rewritten to match MarkProxyAsBroken's proven pattern: one explicit
-- transaction, a single UPDLOCK/HOLDLOCK lookup per IP, and @proxyId
-- reused instead of re-querying IPProxy repeatedly.
ALTER PROCEDURE [dbo].[AddOrUpdateProxy]
	@ip nvarchar(150),
	@port int,
	@lastChecked int,
	@protocols nvarchar(25)
AS
BEGIN
	SET NOCOUNT ON;

	declare @proxyId int

	begin transaction
		select @proxyId = ID from dbo.IPProxy with (UPDLOCK, HOLDLOCK) where IP = @ip

		if @proxyId is not null
		begin
			if exists (select 1 from dbo.IPProxy where ID = @proxyId and Port = @port and lastChecked < @lastChecked)
			begin
				update dbo.IPProxy set lastChecked = @lastChecked, IsBroken = 0 where ID = @proxyId
			end
		end
		else
		begin
			insert into dbo.IPProxy (IP, Port, LastChecked, IsBroken) values (@ip, @port, @lastChecked, 0)
			set @proxyId = SCOPE_IDENTITY()
		end

		create table #ipProtocols(protocol nvarchar(25))
		insert into #ipProtocols
		SELECT VALUE FROM STRING_SPLIT(@protocols, ',');

		insert into dbo.ProxyProtocols(Protocol)
		select p.protocol from #ipProtocols p
		left join dbo.ProxyProtocols pp on p.protocol = pp.Protocol
		where pp.Protocol is null

		insert into dbo.relIpProxyProxyProtocols (IPProxyId, ProxyProtocolId)
		select @proxyId, prot.ID
		from dbo.ProxyProtocols prot
		where prot.Protocol in (select protocol from #ipProtocols)
		and not exists (
			select 1 from dbo.relIpProxyProxyProtocols existing
			where existing.IPProxyId = @proxyId and existing.ProxyProtocolId = prot.ID
		)

		drop table #ipProtocols
	commit transaction
END
GO
