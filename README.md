# pg-fc

A Firecracker rootfs that boots straight into Postgres, with the data directory
on a separate volume mounted at `/workspace`.

## Files

| file | purpose |
|------|---------|
| `Dockerfile` | Debian + Postgres image; ships `init.sh` as `/sbin/init.sh` |
| `init.sh` | PID 1 inside the microVM: mounts pseudo-fs + data volume, init's the cluster, exec's postgres |
| `build-rootfs.sh` | Builds the image and flattens it into a bootable ext4 rootfs |

## Design

Firecracker boots a kernel + a flat rootfs and runs `init=` as PID 1 — there's
no systemd. `init.sh` does the minimal init work and then `exec`s `postgres` so
it inherits PID 1 and gets clean SIGTERM shutdown.

The OS rootfs stays disposable; **all database state lives on `/workspace`**,
which is a second Firecracker drive (`/dev/vdb` by default). On first boot the
volume is formatted ext4 and the cluster is `initdb`'d into
`/workspace/pgdata`; subsequent boots just mount and start.

## Build

`build-rootfs.sh` needs Linux (mkfs.ext4 + loopback mount):

```sh
./build-rootfs.sh pg-rootfs.ext4 2G
```

## Boot

```sh
firecracker --api-sock /tmp/fc.sock   # then configure via the API, or use a config:
```

Key settings:
- `boot-source.boot_args`: include `init=/sbin/init.sh console=ttyS0 reboot=k panic=1`
- `drives`: `vda` = `pg-rootfs.ext4` (root), `vdb` = your persistent data disk
- override the data device with the kernel arg `pgdata_dev=/dev/vdc` if needed

Postgres listens on `0.0.0.0:5432`. Reach it over the VM's tap interface.

## pg-vm-pool (per-schema pooler)

This repo also contains `pg-vm-pool` (`src/`), a connection pooler that fronts
many of these microVMs behind a single Postgres endpoint — **one VM per
schema**. The database name in the client's connection string selects the
schema; the pooler lazily creates/restarts the `pg-<schema>` VM, opens a raw-TCP
iroh tunnel to its Postgres, and splices the connection through.

```sh
cargo run                       # listens on 127.0.0.1:6432 (PG_VM_POOL_LISTEN)
psql "host=127.0.0.1 port=6432 user=postgres dbname=tenant1"   # -> VM pg-tenant1
```

Config via env: `PG_VM_POOL_LISTEN`, `PG_VM_POOL_IMAGE` (default `pg`),
`PG_VM_POOL_USER`, `PG_VM_POOL_PASSWORD` (optional), `PG_VM_POOL_READY_TIMEOUT_SECS`,
`PG_VM_POOL_CONNECT_TIMEOUT_SECS` (tunnel-handshake cap, default 30),
`PG_VM_POOL_DIRECT_CONNECT` (default on).

Requires a local heyvmd with the `POST /sandboxes/:id/tcp-tunnel` endpoint and
`heyo-sdk` ≥ 0.1.5 (`Sandbox::expose_tcp` + `SandboxInfo.guest_ip`). Connect as
`user=postgres`; the VM's `trust` host auth needs no password (see the auth note
in `init.sh`). Set `PG_VM_POOL_USER`/`PG_VM_POOL_PASSWORD` if a VM's Postgres
instead requires password auth (scram/md5) — the pooler uses them for its
readiness probe and per-schema bootstrap connection.

**Direct connect (default):** when the pooler shares the host with the VMs (the
local-daemon deployment), it dials each VM's Postgres directly at its `guest_ip`
over the host tap and skips the iroh tunnel entirely — no relay dependency,
lower latency, faster bring-up. It falls back to a tunnel automatically if the
daemon reports no `guest_ip`. Set `PG_VM_POOL_DIRECT_CONNECT=0` to force the
tunnel path (e.g. if the pooler ever runs on a different machine than the VMs).
