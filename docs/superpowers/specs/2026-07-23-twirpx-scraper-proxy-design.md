# twirpx-scraper: proxy support + credential-based login fallback

## Context

`twirpx-scraper` is one of three ad hoc personal corpus-building containers
deployed on `corpus-host` (172.21.128.103) alongside this repo's own Airflow
pipeline (see project memory, "2026-07-22: ad hoc scraper containers"). It is
not part of this git repo — its source (`parsing_twirpx.py`, `Dockerfile`,
`entrypoint.sh`, `cookies.json`) lives at `C:\Repositories\Grammar\` locally
and `/home/s939/twirpx_scraper/` on the host.

Earlier in this session, two distinct bugs were found and fixed in this
container:

1. A stale Xvfb `/tmp/.X99-lock` left behind by any unclean exit permanently
   wedged the next restart (fixed by adding a cleanup line to `entrypoint.sh`
   and rebuilding).
2. After that fix, monitoring caught a **second, unrelated** problem: every
   login attempt from `corpus-host`'s outbound IP (`89.175.46.44`) now fails
   with `ERR_CONNECTION_CLOSED`/`ERR_CONNECTION_RESET` against twirpx.com. A
   control test against an unrelated Cloudflare-fronted site (discord.com)
   showed the same failure, while a non-Cloudflare site (google.com) worked
   fine — pointing to a Cloudflare IP-reputation block on `89.175.46.44`
   (likely triggered by the scraper's own bot-like automated browsing),
   rather than a twirpx.com-specific block.

This design routes twirpx-scraper's traffic through the same rotating proxy
pool the pipeline's own DAGs already use, so it isn't stuck behind one
flagged IP. Since a proxy's IP will usually differ from whatever IP the
existing `cookies.json` session was created under, the design also adds a
password-based login fallback for when the session cookies no longer work.

## Goals

- twirpx-scraper's Playwright browser routes all traffic through a proxy
  pulled from the same `IPProxy` pool / stored procedures the Airflow DAGs
  use (`paperDownloader.py` / `proxy_repository.py`), so it stops being
  stuck behind one Cloudflare-flagged IP.
- When the existing cookie-based session doesn't work (likely after an IP
  change), the script can fall back to a real username/password login instead
  of just giving up.
- No credential (DB login, twirpx.com login) is ever typed into chat,
  echoed back by the assistant, or committed to git.

## Non-goals

- Proxy rotation *within* a single run. One proxy is selected at container
  startup and used for the whole session; a new one is picked on the next
  restart. Mid-run rotation would require tearing down and rebuilding the
  browser context, likely invalidating the login, for a benefit that doesn't
  clearly outweigh the complexity here.
- Automatic CAPTCHA solving. If Cloudflare or twirpx.com presents a
  challenge that isn't a plain login form, the script logs the failure and
  backs off rather than attempting to solve it.
- Migrating this project into the main git repo's `dags/` package. It stays
  a self-contained ad hoc project, consistent with `grammarwatch`/
  `glottolog`.

## Architecture

```
┌─────────────────────────┐
│ twirpx-scraper container│
│                         │
│  parsing_twirpx.py      │
│    main()               │
│      db_proxy.get_proxy()───────┐
│      chromium.launch(proxy=…)   │
│      authorize()                │
│        cookies → OK? done       │
│        cookies fail → fill      │
│        login form w/ env creds  │
│      … scraping run …           │
│      db_proxy.mark_success/     │
│        mark_broken(ip)          │
└─────────────────────────┼────────┘
                          │ pyodbc, host.docker.internal:1433
                          ▼
                 ┌──────────────────┐
                 │ SQL Server        │
                 │ TextCorpuses DB   │
                 │  IPProxy table    │
                 │  GetLatestFreeProxy│
                 │  MarkProxyAsBroken │
                 │  MarkProxySuccess  │
                 └──────────────────┘
```

The container is not part of the `apache-airflow` docker-compose network, so
it reaches SQL Server the same way other host-level services are reached by
compose containers in reverse: via `host.docker.internal`, requiring
`--add-host=host.docker.internal:host-gateway` on the `docker run` command
(mirroring `x-airflow-common`'s own `extra_hosts` entry), hitting the
`mssql` service's host-published port 1433.

## Components

### 1. New SQL login (`Database/database-v0.29.sql`)

Production's real `configs.json` (checked directly on the host, not the
git-tracked placeholder) shows the DAGs themselves connect as `UID=sa` — there
is no existing scoped-down "airflow" application login in production despite
what the placeholder config implies. Handing this ad hoc scraper's env file
the real SA password would be a disproportionate blast radius for a personal
side project, so this migration creates a dedicated, minimally-privileged
login instead:

```sql
CREATE LOGIN twirpx_readonly WITH PASSWORD = '<CHANGE_ME_ON_APPLY>';
GO
USE TextCorpuses;
GO
CREATE USER twirpx_readonly FOR LOGIN twirpx_readonly;
GO
GRANT EXECUTE ON [dbo].[GetLatestFreeProxy] TO twirpx_readonly;
GRANT EXECUTE ON [dbo].[MarkProxyAsBroken] TO twirpx_readonly;
GRANT EXECUTE ON [dbo].[MarkProxySuccess] TO twirpx_readonly;
```

No other grants. The literal password in the committed file is a
placeholder; the real value is set on production when the migration is
applied (a live DB write — per this repo's established norm, the exact
statement gets named and confirmed before running against production), and
never printed back into chat or a commit.

### 2. `db_proxy.py` (new file, `Grammar/` + `/home/s939/twirpx_scraper/`)

Self-contained — no dependency on this repo's `dags/` package (matches how
`grammarwatch`/`glottolog` are already deployed as fully independent
projects sharing only the Docker host). Mirrors the shape of
`proxy_repository.py`'s methods without importing it:

```python
def get_proxy() -> dict:
    """Returns {'ip', 'port', 'protocol'} via GetLatestFreeProxy.
    Raises RuntimeError if none available."""

def mark_broken(ip: str) -> None: ...
def mark_success(ip: str) -> None: ...
```

Connection built from env vars, all with defaults except the password:

| Env var | Default |
|---|---|
| `MSSQL_HOST` | `host.docker.internal` |
| `MSSQL_PORT` | `1433` |
| `MSSQL_DB` | `TextCorpuses` |
| `MSSQL_USER` | `twirpx_readonly` |
| `MSSQL_PASSWORD` | *(required, no default)* |

### 3. `parsing_twirpx.py` changes

- `main()`: replace `random.choice(PROXY_LIST)` with `db_proxy.get_proxy()`.
  Build Playwright's proxy dict as
  `{"server": f"{protocol}://{ip}:{port}"}` (anonymous proxies, no
  per-proxy credentials, matching the DAGs' usage).
- `authorize()`: unchanged cookie path first. On failure, and only if
  `TWIRPX_USERNAME`/`TWIRPX_PASSWORD` env vars are set (otherwise behaves
  exactly as today — cookie-only), fill the site's inline login form:
  resilient selectors (`input[type=email]`/`input[name*=mail]` for
  username, `input[type=password]` for password, submit button matched by
  visible text "Войти"), then wait for the same `.user-menu .name` success
  indicator already used for the cookie path. On success, re-export cookies
  via `context.cookies()` back to `cookies.json` so subsequent runs mostly
  stay cookie-only.
- After the run (success or failure), call `db_proxy.mark_success(ip)` or
  `db_proxy.mark_broken(ip)` on the proxy that was used, keeping the DB's
  proxy-quality signal accurate the same way the DAGs do.
- If `db_proxy.get_proxy()` raises (no proxy available), or both the cookie
  and password login paths fail, log clearly and `sleep(300)` before
  exiting, so the container's `unless-stopped` restart doesn't hammer the DB
  or the login page every few seconds — the same class of tight-loop problem
  already hit twice this session.

### 4. Dockerfile

Add the same ODBC Driver 18 install steps already used in
`Dockerfile.airflow` (`msodbcsql18`, `unixodbc-dev`, `pyodbc` via
`requirements.txt`).

### 5. Deployment

Recreate the container with:

```
docker run -d --name twirpx-scraper --restart unless-stopped \
  --add-host=host.docker.internal:host-gateway \
  --env-file /home/s939/twirpx_scraper/twirpx.env \
  -v /home/s939/twirpx_scraper:/app \
  twirpx-scraper:latest
```

`twirpx.env` (mode 600, **not** git-tracked — same convention as
`report.env`) holds `MSSQL_PASSWORD`, `TWIRPX_USERNAME`, `TWIRPX_PASSWORD`,
and any non-default connection values. Populated directly by the user via
the `!` shell-passthrough, never typed into a chat message or produced by
the assistant.

## Error handling

- No proxy available → log, sleep 5 min, exit (see above).
- Proxy connects but site login fails (bad/expired cookies **and** no env
  creds, or wrong creds) → log clearly which case it was, exit without a
  long sleep (this is a config problem, not a proxy/rate problem — fast
  restart is fine since it can't succeed until the user fixes credentials
  anyway, but it should be *loud* in the logs so it doesn't look like the
  earlier Xvfb bug).
- Proxy itself is unreachable/times out → treat as a broken proxy
  (`mark_broken`), same failure path already used for connection errors.
- Login form present but has an unexpected shape (e.g. a CAPTCHA challenge
  instead of the plain form) → log and treat as a failed login, no attempt
  to solve it.

## Testing / validation plan

Since the login page's exact form field selectors weren't fully confirmed
(the page-fetch tool used during design lost precise HTML attributes in its
markdown conversion), this needs live verification once corpus-host can
actually reach twirpx.com through a working proxy — it currently can't reach
it at all (the Cloudflare block this design exists to route around).
Concretely:

1. Verify `db_proxy.get_proxy()` in isolation first (small standalone
   script/REPL check) — confirms DB connectivity and the new login's grants
   work before touching the browser flow at all.
2. Run the container once with `TWIRPX_USERNAME`/`PASSWORD` set and
   `cookies.json` deliberately removed/renamed, to force the password path,
   and visually confirm (via logs / a saved screenshot on failure) that the
   form-fill selectors actually match the live page.
3. Confirm `mark_success`/`mark_broken` are actually being called by
   checking `IPProxy.SuccessCount`/`IsBroken` change for the proxy the run
   used.
4. Re-run the earlier crash-loop check (`RestartCount`) after a deliberate
   `docker kill` to confirm the new failure paths back off instead of
   tight-looping, same verification style already used for the Xvfb fix.
