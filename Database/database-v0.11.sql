USE [TextCorpuses]
GO

-- Tracks each proxy's real track record so selection can prefer proxies
-- that have actually completed real downloads before, rather than only
-- ordering by lastChecked (which just reflects when a proxy was last
-- imported/refreshed, not whether it has ever worked for a real transfer).
ALTER TABLE [dbo].[IPProxy] ADD [SuccessCount] int NOT NULL DEFAULT 0
GO

CREATE PROCEDURE [dbo].[MarkProxySuccess]
	@ip nvarchar(150)
AS
BEGIN
	SET NOCOUNT ON;
	UPDATE [dbo].[IPProxy]
	SET SuccessCount = SuccessCount + 1
	WHERE IP = @ip
END
GO

-- Both selection procedures now rank by proven track record first
-- (SuccessCount DESC), falling back to lastChecked DESC so freshly
-- imported/refreshed proxies with no track record yet are still tried.
ALTER PROCEDURE [dbo].[GetLatestProxy]
AS
BEGIN
	SET NOCOUNT ON;

	SELECT TOP 1 [IP], [Port], protocols.Protocol from [dbo].[IPProxy] proxy
	inner join dbo.relIpProxyProxyProtocols rel on proxy.ID = rel.IPProxyId
	INNER JOIN dbo.ProxyProtocols protocols on rel.ProxyProtocolId = protocols.ID
	where proxy.IsBroken = 0
	order by proxy.SuccessCount desc, proxy.lastChecked desc
END
GO

ALTER PROCEDURE [dbo].[GetLatestFreeProxy]
AS
BEGIN
	SET NOCOUNT ON;

	SELECT TOP 1 [IP], [Port], protocols.Protocol from [dbo].[IPProxy] proxy
	inner join dbo.relIpProxyProxyProtocols rel on proxy.ID = rel.IPProxyId
	INNER JOIN dbo.ProxyProtocols protocols on rel.ProxyProtocolId = protocols.ID
	where proxy.IsBroken = 0 and proxy.IP not like '%@%'
	order by proxy.SuccessCount desc, proxy.lastChecked desc
END
GO
