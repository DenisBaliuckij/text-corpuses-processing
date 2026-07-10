USE [TextCorpuses]
GO

-- Removes the arXiv rate limiter introduced in v0.6, since direct
-- (proxy-less) arXiv access proved unreliable in practice and the pipeline
-- reverted to routing arXiv requests through the proxy pool like every
-- other source.
DROP PROCEDURE IF EXISTS [dbo].[ReserveArxivRequestSlot]
GO

DROP TABLE IF EXISTS [dbo].[ArxivRateLimit]
GO
