# SQL Server data migration to NVMe drive

## Problem

The production host's single disk (`sda`, 3.6TB, backs the OS, SQL Server,
FTP storage, Docker, and swap) is running at 80-99% utilization with
170-450ms write latency. `vmstat` shows 8-10 processes permanently blocked
on I/O at any moment, and `sqlservr` is pegged at ~190% CPU. This is the
current ceiling on `pdf_downloading` throughput - every URL claim, proxy
churn write, and state update is a SQL Server write contending for the
same saturated disk.

A second disk, `nvme0n1` (953.9GB), is attached to the host and was
completely unpartitioned/unused until this work.

## Goal

Move SQL Server's data and log files off the contended `sda` onto the
idle NVMe drive, to relieve the disk bottleneck without touching
application code or `CONCURRENCY` settings (which would only add more
contention on `sda` if raised further).

## Scope

SQL Server's data/log files only (the `mssql` container's bind-mounted
volumes). Not in scope: FTP-served PDF storage, Docker's storage root,
or the swap files added earlier this session - those can be considered
separately if disk pressure persists after this change.

## Design

### Phase 1 - Partition and mount (root, done)

```bash
sudo parted /dev/nvme0n1 --script mklabel gpt mkpart primary ext4 0% 100%
sudo mkfs.ext4 /dev/nvme0n1p1
sudo mkdir -p /mnt/nvme-mssql
sudo mount /dev/nvme0n1p1 /mnt/nvme-mssql
sudo chown s939:s939 /mnt/nvme-mssql
echo "<uuid> /mnt/nvme-mssql ext4 defaults 0 2" | sudo tee -a /etc/fstab
```

Verified: `/dev/nvme0n1p1` (938G, ext4) mounted at `/mnt/nvme-mssql`,
owned by `s939`. `/etc/fstab` entry confirmed present:
`a90bc68e-f478-4391-b34a-fec4ca9162c7 /mnt/nvme-mssql ext4 defaults 0 2`.

### Phase 2 - Cutover (non-root, this session)

The `mssql` service in `~/apache-airflow/docker-compose.yaml` currently
binds:

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/mssql-data:/var/opt/mssql
  - ${AIRFLOW_PROJ_DIR:-.}/mssql-backups:/var/opt/mssql/backup
```

Both host paths resolve to `~/apache-airflow/mssql-data` (3.5GB) and
`~/apache-airflow/mssql-backups` (287MB) on `sda`.

Steps:

1. Run the existing `mssql_backup.sh` once manually, as an extra safety
   snapshot on top of the nightly 2am cron backup.
2. `docker compose stop mssql` (from `~/apache-airflow`).
3. `rsync -a` both directories from their current `sda` paths into
   `/mnt/nvme-mssql/mssql-data/` and `/mnt/nvme-mssql/mssql-backups/`.
4. Edit `docker-compose.yaml`'s two `mssql` volume lines to point at
   `/mnt/nvme-mssql/mssql-data` and `/mnt/nvme-mssql/mssql-backups`.
5. `docker compose up -d mssql`; wait for the healthcheck
   (`SELECT 1` via sqlcmd) to pass.
6. Verify data integrity: compare `PdfDocuments` and `IPProxy` row
   counts (and any other cheap sanity count) from before the stop
   against after the restart - must match exactly.
7. Confirm disk I/O relief: re-check `iostat -x` on `sda` and `sqlservr`
   CPU/wait after a few minutes of normal traffic.

### Rollback

The original `mssql-data`/`mssql-backups` directories on `sda` are left
in place (not deleted) for several days after cutover. If anything goes
wrong, revert the two volume lines in `docker-compose.yaml` to their
original `sda` paths and `docker compose up -d mssql` again - the old
data is untouched and immediately usable.

Only after the new location has run stably (a few days, general
guideline - no fixed threshold) should the old `sda` copies be deleted
to reclaim space.

## Risk / downtime

- `docker-compose.yaml` is not git-tracked (hand-maintained on this
  host, per existing deployment topology) - this change is a direct
  edit on the host, not a git commit.
- Expected downtime: well under a minute for the stop/rsync/restart
  given the small (~3.8GB) data size.
- Tasks/DAGs that hit the DB during the outage window will fail their
  current attempt and retry on the next `@continuous` tick or cron
  fire - this is existing, already-safe behavior (state is not advanced
  on a DB error), not a new failure mode introduced by this migration.
- No application code changes required.

## Out of scope / follow-ups

- Moving FTP storage or Docker's storage root to NVMe, if disk pressure
  persists after this change.
- Investigating why/when the NVMe drive was originally attached to the
  host (couldn't be determined without `sudo` - `dmesg`/`smartctl`
  access - during this session).

## Completion (2026-07-16)

Executed successfully, with two real problems hit and fixed along the
way (both caught before any data loss):

1. `rsync` silently failed to copy the actual `.mdf`/`.ldf` files
   (owned by uid 10001, unreadable by `s939`) - switched to `docker cp`,
   which reads through the running/stopped container correctly.
2. `docker compose stop` doesn't work on this container at all -
   `restart: always` overrides manual stops, causing an unclean SIGKILL
   + crash-recovery cycle instead of a graceful shutdown. Fixed by
   temporarily setting `restart: "no"`, stopping cleanly, then restoring
   `restart: always` after the cutover.
3. `docker cp` re-owns extracted files to the invoking user (`s939`,
   uid 1000) rather than preserving the source uid (10001) - needed one
   manual `sudo chown -R 10001:10001` before the container could start
   at the new location.

Data integrity verified via row counts at every step (`PdfDocuments`,
`IPProxy`, `ServiceState`, `GraphConstructionJob`) - all progressed
normally throughout, no gaps or resets.

Follow-up result: `sda` remained at 80-94% utilization immediately
after this migration alone - SQL Server was not the only major
consumer. See the sibling Docker-root-migration spec, which was needed
in addition to fully resolve the bottleneck (`sda` dropped to
1.6-4.8% only after both migrations were complete).
