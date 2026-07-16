# Docker storage root migration to NVMe drive

## Problem

Moving SQL Server's data/log files to the NVMe drive (see the sibling
spec, `2026-07-16-mssql-nvme-migration-design.md`) did not fully relieve
`sda` - it remained at 80-94% utilization afterward. Remaining
contributors still on `sda`: Docker's storage root (`/var/lib/docker`,
default location - container image layers, writable layers, and named
volumes), the swap files, and FTP-served PDF storage.

This spec covers moving Docker's storage root specifically.

## Scope estimate

Via `docker system df -v` (no root needed):
- Images: ~16GB unique, dominated by `open-webui` (6.9GB unique) - the
  Ollama chat UI, unrelated to this pipeline but sharing the host.
- Volumes: ~2.75GB (`apache-airflow_postgres-db-volume` 1.63GB,
  `open-webui-data` 1.12GB).
- Container writable layers: small, under 500MB combined.
- Total: roughly 19-20GB. A direct `du -sh /var/lib/docker` as `s939`
  fails with Permission denied (root-owned throughout), so this is an
  estimate, not an exact figure.

NVMe has 891GB free after the mssql migration - ample room.

## Why this is a bigger undertaking than the mssql migration

- Relocating Docker's data-root requires stopping the Docker **daemon**
  itself (`systemctl stop docker`), which takes down every container on
  the host simultaneously: all 7 Airflow containers, `mssql`, `postgres`,
  `redis`, `nginx-5335`, the custom-query `webui`, and **`open-webui`
  (Ollama chat UI)** - a service unrelated to this pipeline that shares
  the host.
- No `docker cp` trick is available this time. That worked for SQL
  Server because the container could still read its own files while
  running. Docker's own storage (overlay2 layers - root-owned files,
  hardlinks for layer deduplication, and extended attributes for
  whiteout/opaque markers) needs the daemon stopped to copy safely, and
  reading it at all requires root.
- The whole operation - stop, copy, reconfigure, restart - is root-only
  end to end. Unlike the mssql migration, there is no non-root portion
  for me to execute; this needs to be run by Denis via `sudo`.

## Design

### Phase 1 - Stop Docker (full host outage begins)

```bash
sudo systemctl stop docker.socket docker.service
```

Stopping the socket first prevents socket-activation from silently
respawning the daemon mid-copy.

### Phase 2 - Copy the storage root

```bash
sudo mkdir -p /mnt/nvme-mssql/docker-data
sudo rsync -aHAX --info=progress2 /var/lib/docker/ /mnt/nvme-mssql/docker-data/
```

`-H` (hardlinks) and `-X` (extended attributes) are not optional here -
overlay2 depends on both for layer deduplication and whiteout/opaque
directory markers. A plain `-a` would silently produce a working-looking
but subtly wrong copy (ballooned size at best, broken overlay semantics
at worst). `-A` preserves ACLs if any are in use.

Do **not** delete or rename the original `/var/lib/docker` yet - it stays
in place as the immediate rollback path (see below).

### Phase 3 - Point Docker at the new location

```bash
sudo mkdir -p /etc/docker
echo '{"data-root": "/mnt/nvme-mssql/docker-data"}' | sudo tee /etc/docker/daemon.json
sudo systemctl start docker.socket docker.service
```

### Phase 4 - Verify (outage ends here if successful)

```bash
docker info | grep 'Docker Root Dir'   # should show /mnt/nvme-mssql/docker-data
docker ps -a                            # all containers should be present
```

Containers with `restart: always`/`unless-stopped` policies should
self-start when the daemon comes up. If any are missing or stopped:

```bash
cd ~/apache-airflow && docker compose up -d
```

Then confirm application-level health:
- `mssql` data intact (row counts, same check used for the previous
  migration)
- `open-webui` and the custom-query `webui` reachable
- Airflow scheduler/worker/dag-processor healthy, DAGs resuming

### Rollback

If Phase 3/4 fails, revert by deleting `/etc/docker/daemon.json` and
restarting the daemon - it falls back to the untouched original
`/var/lib/docker`:

```bash
sudo rm /etc/docker/daemon.json
sudo systemctl start docker.socket docker.service
```

Only after the new location has run stably for several days should the
original `/var/lib/docker` be removed to reclaim `sda` space.

## Risk / downtime

- Full-host container outage for the duration of the copy. Unlike the
  ~3.8GB mssql copy, ~19-20GB at this host's demonstrated disk speeds
  (seen as low as 0.687MB/sec under contention, though a fully-stopped
  Docker daemon means no other process is competing for `sda` during the
  copy itself) could take anywhere from a couple of minutes to
  significantly longer - hard to bound precisely given how variable this
  host's I/O has been.
- Every DAG task attempted during the outage will fail and retry on its
  next tick, per existing (already-safe) behavior.
- `open-webui`/Ollama chat and the custom-query `webui` will be
  unreachable for the duration - flagged since these are used outside
  this pipeline's scope.
- This entire migration is root-only; I cannot execute any step of it
  myself (unlike the mssql migration, where the copy/restart portions
  were mine to run). Denis runs Phases 1-3 directly; I verify Phase 4
  and application-level health afterward.

## Out of scope / follow-ups

- Moving FTP-served PDF storage or the swap files to NVMe, if `sda`
  pressure persists after this change too.
