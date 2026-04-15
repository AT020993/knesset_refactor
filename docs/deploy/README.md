# VPS deployment — systemd timer for the ETL + snapshot pipeline

**Target:** Linux (systemd) host. macOS dev machines use launchd instead — see the project's root CLAUDE.md rule on OS-managed long-lived services.

**Scope:** this directory delivers the unit files. Actually installing them on the Hetzner CX22 VPS is **Phase 5 work**, not Phase 0.

## Files

| File | Purpose |
|---|---|
| [`knesset-etl.service`](./knesset-etl.service) | One-shot: refresh all OData tables → export Parquet snapshot bundle. |
| [`knesset-etl.timer`](./knesset-etl.timer) | Fires the service every 6h at :30 past the hour. |

## Why systemd, not APScheduler

- One-shot unit + calendar timer = zero new Python dependency.
- OS-managed restart, respawn, and logging via `journalctl`.
- Matches the project's global rule for long-lived services (see root CLAUDE.md).
- Timer cadence can be tuned without a code change.

## What the service does

On every fire:

1. **Refresh the warehouse** — `python -m src.cli refresh` (no args = all relevant OData tables).
2. **Export the snapshot bundle** — `python -m data.snapshots.exporter --warehouse ... --output-dir ...`.

Both steps are idempotent; the snapshot exporter is atomic per file and commits via `manifest.json`. Interrupting mid-run (VPS reboot, SIGTERM) never leaves a partially-visible bundle for FastAPI consumers.

## Install on the VPS (Phase 5)

Assumes the repo is at `/opt/knesset_refactor`, a `knesset` user/group exists, and `/var/lib/knesset/` holds `warehouse.duckdb` + `snapshots/` with `knesset:knesset` ownership.

```bash
# Copy unit files into place
sudo install -m 0644 docs/deploy/knesset-etl.service /etc/systemd/system/
sudo install -m 0644 docs/deploy/knesset-etl.timer   /etc/systemd/system/

# (Optional but recommended) lint before loading
sudo systemd-analyze verify /etc/systemd/system/knesset-etl.{service,timer}

# Reload the daemon, enable + start the timer
sudo systemctl daemon-reload
sudo systemctl enable --now knesset-etl.timer

# Trigger a manual one-shot to confirm end-to-end
sudo systemctl start knesset-etl.service
```

## Observability

```bash
# Next fire time
systemctl list-timers knesset-etl.timer

# Live log tail for the service
journalctl --unit knesset-etl.service -f

# Last 200 lines across all fires, with timestamps
journalctl --unit knesset-etl.service --since -24h --no-pager

# Manifest freshness (from any client)
curl -s https://api.<domain>/v1/meta/freshness | jq '.generated_at_utc'
```

## Restart pattern

```bash
# Graceful: sends SIGTERM, waits for exit, respawns
sudo systemctl restart knesset-etl.service
```

## Path assumptions in the unit

The unit hardcodes:

- `WorkingDirectory=/opt/knesset_refactor`
- `PYTHONPATH=/opt/knesset_refactor/src`
- Interpreter: `/opt/knesset_refactor/.venv/bin/python`
- Warehouse: `/var/lib/knesset/warehouse.duckdb`
- Snapshots: `/var/lib/knesset/snapshots`

If the VPS layout differs, drop-in overrides are the right tool — don't edit the committed unit:

```bash
sudo systemctl edit knesset-etl.service
# adds /etc/systemd/system/knesset-etl.service.d/override.conf
```

## Local verification (what "passes" on the dev machine)

macOS has no `systemd-analyze` or `systemctl`; the unit file syntax can't be linted locally. What *is* verifiable before deploy:

```bash
# From the repo root, exercise the ExecStart pipeline manually:
PYTHONPATH=./src .venv/bin/python -m src.cli refresh          # step 1
PYTHONPATH=./src .venv/bin/python -m data.snapshots.exporter \
    --warehouse data/warehouse.duckdb \
    --output-dir data/snapshots/                              # step 2

ls data/snapshots/                                           # 7 parquets + manifest.json
jq '.generated_at_utc, .warehouse_mtime_utc' data/snapshots/manifest.json
```

If both commands succeed and leave a consistent snapshot dir on the dev machine, the VPS systemd path is expected to work — the unit just wraps the same two shell invocations with environment, working dir, and scheduling.
