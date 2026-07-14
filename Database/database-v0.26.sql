USE [TextCorpuses]
GO

-- GetLatestProxy/GetLatestFreeProxy previously picked TOP 1 deterministically
-- (SuccessCount DESC, LastChecked DESC), so every concurrent pdf_downloading
-- worker independently landed on the exact same "champion" proxy - raising
-- CONCURRENCY just piled more simultaneous connections onto one IP instead
-- of spreading load across the pool. Confirmed 2026-07-14: task logs showed
-- heavy ProxyError/timeout/connection-reset volume even at CONCURRENCY=16,
-- and working proxies die and get deleted (MarkProxyAsBroken) within the
-- same run. Now picks randomly among the top 20 candidates by the same
-- ranking, so concurrent workers fan out across multiple proven-good
-- proxies instead of hammering a single one.
ALTER PROCEDURE [dbo].[GetLatestProxy]
AS
BEGIN
	SET NOCOUNT ON;

	SELECT TOP 1 IP, Port, Protocol
	FROM (
		SELECT TOP 20 proxy.[IP], proxy.[Port], protocols.Protocol
		FROM [dbo].[IPProxy] proxy
		INNER JOIN dbo.relIpProxyProxyProtocols rel ON proxy.ID = rel.IPProxyId
		INNER JOIN dbo.ProxyProtocols protocols ON rel.ProxyProtocolId = protocols.ID
		WHERE proxy.IsBroken = 0
		ORDER BY proxy.SuccessCount DESC, proxy.LastChecked DESC
	) candidates
	ORDER BY NEWID()
END
GO

ALTER PROCEDURE [dbo].[GetLatestFreeProxy]
AS
BEGIN
	SET NOCOUNT ON;

	SELECT TOP 1 IP, Port, Protocol
	FROM (
		SELECT TOP 20 proxy.[IP], proxy.[Port], protocols.Protocol
		FROM [dbo].[IPProxy] proxy
		INNER JOIN dbo.relIpProxyProxyProtocols rel ON proxy.ID = rel.IPProxyId
		INNER JOIN dbo.ProxyProtocols protocols ON rel.ProxyProtocolId = protocols.ID
		WHERE proxy.IsBroken = 0 AND proxy.IP NOT LIKE '%@%'
		ORDER BY proxy.SuccessCount DESC, proxy.LastChecked DESC
	) candidates
	ORDER BY NEWID()
END
GO
