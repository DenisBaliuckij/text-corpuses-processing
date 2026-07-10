USE [TextCorpuses]
GO

-- Adds a proxy-selection path that excludes the shared paid (BrightData)
-- proxy. BrightData's ISP proxy has proven unreliable for actual file
-- downloads (intermittent stalls/timeouts unrelated to file size or
-- destination), so file-transfer DAGs should draw from the free proxy
-- pool instead, while URL-discovery DAGs continue to prefer BrightData via
-- the existing GetLatestProxy. BrightData's IP field is stored as the
-- compound "user:pass@host" string (unlike free proxies, which are plain
-- dotted IPs), so filtering out rows containing '@' reliably excludes it
-- without a schema change.
CREATE PROCEDURE [dbo].[GetLatestFreeProxy]
AS
BEGIN
	SET NOCOUNT ON;

	SELECT TOP 1 [IP], [Port], protocols.Protocol from [dbo].[IPProxy] proxy
	inner join dbo.relIpProxyProxyProtocols rel on proxy.ID = rel.IPProxyId
	INNER JOIN dbo.ProxyProtocols protocols on rel.ProxyProtocolId = protocols.ID
	where proxy.IsBroken = 0 and proxy.IP not like '%@%'
	order by proxy.lastChecked desc
END
GO
