-- Creates a minimally-privileged SQL login for the twirpx-scraper ad hoc
-- container's proxy lookups, so it doesn't need the DAGs' full 'sa' access
-- (confirmed via the real production configs.json that the DAGs themselves
-- connect as 'sa' - there is no existing scoped-down login to reuse).
-- The placeholder password below is never applied as-is: the real
-- production password is generated fresh and substituted at deploy time
-- (see docs/superpowers/plans/2026-07-23-twirpx-scraper-proxy-plan.md, Task 5).
USE [master]
GO
CREATE LOGIN [twirpx_readonly] WITH PASSWORD = N'<CHANGE_ME_ON_APPLY>', CHECK_POLICY = ON;
GO
USE [TextCorpuses]
GO
CREATE USER [twirpx_readonly] FOR LOGIN [twirpx_readonly];
GO
GRANT EXECUTE ON [dbo].[GetLatestFreeProxy] TO [twirpx_readonly];
GO
GRANT EXECUTE ON [dbo].[MarkProxyAsBroken] TO [twirpx_readonly];
GO
GRANT EXECUTE ON [dbo].[MarkProxySuccess] TO [twirpx_readonly];
GO
