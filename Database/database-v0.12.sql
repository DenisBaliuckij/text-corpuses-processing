USE [TextCorpuses]
GO

-- Fixes a regression introduced in v0.11: AddOrUpdateProxy's
-- "insert into IPProxy values(@ip, @port, @lastChecked, 0)" used a bare
-- VALUES clause with no column list, which requires a value for every
-- non-identity column. Adding IPProxy.SuccessCount in v0.11 broke this -
-- every insert for a genuinely new proxy IP started failing with
-- "Column name or number of supplied values does not match table
-- definition", since SuccessCount's DEFAULT only applies when a column
-- is explicitly omitted from a named column list.
ALTER PROCEDURE [dbo].[AddOrUpdateProxy]
	@ip nvarchar(150) ,
	@port int,
	@lastChecked int,
	@protocols nvarchar(25)
AS
BEGIN
	SET NOCOUNT ON;

	if exists(select * from [dbo].IPProxy where [IP] = @ip and [Port] = @port and lastChecked<@lastChecked)
	begin
		update [dbo].IPProxy
		set lastChecked = @lastChecked, IsBroken = 0
		where [IP] = @ip and [Port] = @port
	end

	if not exists (select * from [dbo].IPProxy where [IP] = @ip)
	begin
		insert into [dbo].IPProxy (IP, Port, LastChecked, IsBroken)
		values(@ip, @port, @lastChecked, 0)
	end

	create table #ipProtocols(protocol nvarchar(25))
	insert into #ipProtocols
	SELECT VALUE FROM STRING_SPLIT(@protocols, ',');

	insert into dbo.ProxyProtocols(Protocol)
	select p.protocol from #ipProtocols p
	left join dbo.ProxyProtocols pp on p.protocol = pp.Protocol
	where pp.Protocol is null

	insert into dbo.relIpProxyProxyProtocols (IPProxyId, ProxyProtocolId)
	select prox.ID, prot.ID from
	(select * from [dbo].IPProxy where [IP] = @ip and [Port] = @port) prox
	left join (select * from dbo.ProxyProtocols where protocol in (select * from #ipProtocols)) prot on 1=1
	where prot.ID is not null and not exists (
		select 1 from dbo.relIpProxyProxyProtocols existing
		where existing.IPProxyId = prox.ID and existing.ProxyProtocolId = prot.ID
	)

	drop table #ipProtocols
END
GO
