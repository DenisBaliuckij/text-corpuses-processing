USE [TextCorpuses]
GO

-- Supports the new validate_proxies DAG: returns the top-N proxies by
-- proven track record (the same ranking GetLatestProxy uses), so the
-- validation DAG tests the proxies that are actually being selected for
-- real downloads rather than the whole pool.
CREATE PROCEDURE [dbo].[GetTopProxiesForValidation]
	@topN int = 50
AS
BEGIN
	SET NOCOUNT ON;

	SELECT TOP (@topN) proxy.[IP], proxy.[Port], protocols.Protocol
	FROM dbo.IPProxy proxy
	INNER JOIN dbo.relIpProxyProxyProtocols rel ON proxy.ID = rel.IPProxyId
	INNER JOIN dbo.ProxyProtocols protocols ON rel.ProxyProtocolId = protocols.ID
	WHERE proxy.IsBroken = 0
	ORDER BY proxy.SuccessCount DESC, proxy.LastChecked DESC
END
GO
