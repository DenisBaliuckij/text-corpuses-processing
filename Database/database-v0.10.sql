USE [TextCorpuses]
GO

-- AddOrUpdateProxy's insert into relIpProxyProxyProtocols had no existence
-- check at all, so every re-encounter of an already-known proxy (very
-- common: the same IPs show up repeatedly across geonode/proxydb/
-- proxyscrape/free-proxy-list/brightdata refresh cycles) inserted another
-- duplicate relationship row. This had ballooned to 351,476 rows for only
-- 5,393 distinct proxies, and made GetLatestProxy/GetLatestFreeProxy's join
-- take ~4.5s per call (called on every single download attempt).

-- One-time cleanup: collapse existing duplicates down to one row per
-- (IPProxyId, ProxyProtocolId) pair before adding the constraint that
-- prevents new ones.
;WITH ranked AS (
	SELECT
		[IPProxyId], [ProxyProtocolId],
		ROW_NUMBER() OVER (PARTITION BY [IPProxyId], [ProxyProtocolId] ORDER BY (SELECT NULL)) AS rn
	FROM dbo.relIpProxyProxyProtocols
)
DELETE FROM ranked WHERE rn > 1
GO

-- Belt-and-suspenders: make it impossible to insert a duplicate
-- (IPProxyId, ProxyProtocolId) pair again, regardless of what any stored
-- procedure does.
CREATE UNIQUE INDEX [UQ_relIpProxyProxyProtocols_Proxy_Protocol]
ON dbo.relIpProxyProxyProtocols ([IPProxyId], [ProxyProtocolId])
GO

-- Fix AddOrUpdateProxy to only insert a relationship row when it doesn't
-- already exist.
ALTER PROCEDURE [dbo].[AddOrUpdateProxy]

	-- Add the parameters for the stored procedure here
	@ip nvarchar(150) ,
	@port int,
	@lastChecked int,
	@protocols nvarchar(25)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	if exists(select * from [dbo].IPProxy where [IP] = @ip and [Port] = @port and lastChecked<@lastChecked)
	begin
		update [dbo].IPProxy
		set lastChecked = @lastChecked, IsBroken = 0
		where [IP] = @ip and [Port] = @port
	end

	if not exists (select * from [dbo].IPProxy where [IP] = @ip)
	begin
		insert into [dbo].IPProxy
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
	where not exists (
		select 1 from dbo.relIpProxyProxyProtocols existing
		where existing.IPProxyId = prox.ID and existing.ProxyProtocolId = prot.ID
	)

	drop table #ipProtocols
END
GO
