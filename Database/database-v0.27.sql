USE [TextCorpuses]
GO

-- Root cause of the persistent proxy-pool churn (pool stuck at ~28 rows,
-- SuccessCount never rising above 0): GetTopProxiesForValidation had no
-- age filter, so it kept re-testing "the 50 most recently imported"
-- proxies (SuccessCount ties at 0, so LastChecked DESC dominates the
-- ranking) every single minute. A freshly-imported proxy already passed
-- a real test in proxyValidator.validate_and_import moments earlier, but
-- validate_proxies would re-test it again within a minute and delete it
-- on any failure - often before pdf_downloading even finished a real
-- transfer through it and called MarkProxySuccess. Confirmed live:
-- 2026-07-15, all 28 IPProxy rows had ages under 11 minutes, none older.
--
-- Adding a grace period so validate_proxies only re-tests proxies old
-- enough to have had a real chance to be used first. 300s is sized
-- against pdf-downloading-dag.py's per-request timeout=30s plus room for
-- large multi-chunk transfers to complete.
ALTER PROCEDURE [dbo].[GetTopProxiesForValidation]
	@topN int = 50,
	@minAgeSeconds int = 300
AS
BEGIN
	SET NOCOUNT ON;

	SELECT TOP (@topN) proxy.[IP], proxy.[Port], protocols.Protocol
	FROM dbo.IPProxy proxy
	INNER JOIN dbo.relIpProxyProxyProtocols rel ON proxy.ID = rel.IPProxyId
	INNER JOIN dbo.ProxyProtocols protocols ON rel.ProxyProtocolId = protocols.ID
	WHERE proxy.IsBroken = 0
		AND proxy.LastChecked < DATEDIFF(SECOND, '1970-01-01', GETUTCDATE()) - @minAgeSeconds
	ORDER BY proxy.SuccessCount DESC, proxy.LastChecked DESC
END
GO
