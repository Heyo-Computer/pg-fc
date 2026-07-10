# pg-fc

This repo has two parts that fit together: a **Firecracker build** that
produces a rootfs image booting straight into Postgres, and **pg-vm-pool**, a
connection pooler that runs many of those microVMs behind a single Postgres
endpoint — one VM per schema, created and stopped/restarted on demand. The
Firecracker image is the unit the pooler manages; the pooler is what a real
client actually connects to.

## Prerequisites

Linux only — Firecracker needs KVM, so there's no macOS host support.

**To build the Postgres rootfs image** (`build-rootfs.sh`):

| dependency | why |
|---|---|
| Docker | builds the image from `Dockerfile` before it's flattened |
| `e2fsprogs` (`mkfs.ext4`) | formats the output image |
| root/`sudo` | needed to loopback-mount the image while the container fs is exported into it |

**To run `pg-vm-pool` and boot the VMs it manages:**

| dependency | why |
|---|---|
| Rust (edition 2024 toolchain) | builds `pg-vm-pool` (`cargo build --release`) |
| Firecracker + KVM access (`/dev/kvm`, user in the `kvm` group) | actually boots each per-schema microVM |
| `heyvm` / `heyvmd` (from the sibling `heyo` project) | the VM control plane: `heyvmd` (or `heyvm --api --port 34099`) serves the local sandbox HTTP API pg-vm-pool drives, and the `heyvm` CLI builds the `pg` image (`heyvm mvm build`) |
| `heyo-sdk` crate, `>= 0.1.5` | Rust client for that API; pulled automatically by `cargo build` from crates.io, already pinned in `Cargo.toml` — no separate install needed |

## Postgres VM

A Firecracker rootfs that boots straight into Postgres, with the data directory
on a separate volume mounted at `/workspace`.

### Files

| file | purpose |
|------|---------|
| `Dockerfile` | Debian + Postgres image; ships `init.sh` as `/init.sh` |
| `init.sh` | PID 1 inside the microVM: mounts pseudo-fs + data volume, init's the cluster, exec's postgres |
| `build-rootfs.sh` | Builds the image and flattens it into a bootable ext4 rootfs |

### Design

Firecracker boots a kernel + a flat rootfs and runs `init=` as PID 1 — there's
no systemd. `init.sh` does the minimal init work and then `exec`s `postgres` so
it inherits PID 1 and gets clean SIGTERM shutdown.

The OS rootfs stays disposable; **all database state lives on `/workspace`**,
which is a second Firecracker drive (`/dev/vdb` by default). On first boot the
volume is formatted ext4 and the cluster is `initdb`'d into
`/workspace/pgdata`; subsequent boots just mount and start.

### Build

`build-rootfs.sh` needs Linux (mkfs.ext4 + loopback mount):

```sh
./build-rootfs.sh pg-rootfs.ext4 2G
```

### Boot

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

### Using the pooler

Prereqs: a running local heyvmd (`heyvm --api --port 34099`) with the
`POST /sandboxes/:id/tcp-tunnel` endpoint, `heyo-sdk` ≥ 0.1.5, and the `pg`
image built (`heyvm mvm build --local-only -f Dockerfile --name pg`).

```sh
cargo build --release
target/release/pg-vm-pool       # listens on 127.0.0.1:6432 (PG_VM_POOL_LISTEN)

# The dbname selects (and lazily creates) the VM — one per schema:
psql "host=127.0.0.1 port=6432 user=postgres dbname=tenant1"   # -> VM pg-tenant1
psql "host=127.0.0.1 port=6432 user=postgres dbname=tenant2"   # -> VM pg-tenant2
```

First connect to a new schema boots a VM (~2s); reconnects reuse or restart it.
If Postgres dies while its VM stays up (OOM kill, segfault — the VM's PID 1 is
a shell, so the sandbox still reports running), the pooler notices on the next
connect: a short probe distinguishes a dead postmaster (silent port) from one
that's alive but recovering (answers `57P03` during WAL replay), and only the
former triggers an automatic stop/start of the VM — a fresh boot re-runs
`init.sh`, which relaunches Postgres.
Each schema's data lives on its VM's persistent disk and survives stops,
restarts, and idle reaping. Before an idle stop the pooler issues a
`CHECKPOINT` over its warm connection, so the unclean VM kill loses no
acknowledged commits (the VMs run `synchronous_commit=off`) and the next boot
skips WAL replay entirely. The schema→VM binding is persisted in
`PG_VM_POOL_STATE_FILE` (default `~/.heyo/pg-vm-pool/registry.tsv`) so it also
survives pooler restarts.

Connect as `user=postgres`; the VM image's `trust` host auth needs no password
(see the auth note in `init.sh`). `PG_VM_POOL_PASSWORD` does double duty:

- it's what the pooler itself uses for its readiness probe and per-schema
  bootstrap connection, if a VM's Postgres requires password auth (scram/md5)
  instead of `trust`;
- and, separately, if set it's also the password the pooler **requires from
  clients** (a plain `AuthenticationCleartextPassword` challenge) before it
  proxies them anywhere — see "Client auth" below. Unset means no client auth
  gate at all: fine on a loopback-only `PG_VM_POOL_LISTEN`, not once it's
  reachable from elsewhere.

Config via env (all optional):

| var | default | meaning |
|-----|---------|---------|
| `PG_VM_POOL_LISTEN` | `127.0.0.1:6432` | client listen address |
| `PG_VM_POOL_IMAGE` | `pg` | Firecracker image per schema |
| `PG_VM_POOL_SIZE_CLASS` | `micro` | VM resource tier for every schema's VM: `micro` (0.25 CPU, 512MB), `mini` (0.5 CPU, 1GB), `small` (1 CPU, 2GB), `medium` (2 CPU, 4GB), `large` (4 CPU, 8GB) |

Postgres inside each VM **tunes itself to the VM's resources at every boot**:
`init.sh` reads live RAM/vCPUs/disk and regenerates
`$PGDATA/heyvm-tuning.conf` (`shared_buffers` = ¼ RAM, `work_mem`,
`maintenance_work_mem`, WAL sizing from the data disk, parallel workers from
vCPUs), so one image serves every size class and a VM that changes size class
picks up correct values on its next start. The profile is single-tenant and
ingest-friendly: `wal_level=minimal` + `wal_compression=lz4` (no per-VM
replicas), `synchronous_commit=off` (commits already ride WAL crash recovery
— the pooler stop-kills VMs), SSD plan costs, JIT off. Manual overrides go in
`postgresql.conf`, which is read after the include and therefore wins.

The image also hardens against ingest memory spikes (the classic "big upload
OOM-kills Postgres inside a live VM" failure): `init.sh` creates a swapfile on
the data disk (sized to RAM, capped at 2GB / an eighth of the disk) as an
emergency spillway, switches to strict overcommit (`vm.overcommit_memory=2`,
per the Postgres docs) so an oversized allocation fails just that one query
with a clean `out of memory` instead of summoning the OOM killer, and shields
the postmaster (`oom_score_adj -900`, with `PG_OOM_ADJUST_FILE` resetting
backends to 0) so if the killer runs anyway it takes a recoverable backend,
not the whole cluster. `temp_file_limit` (¼ disk) keeps a runaway sort's
spill files from filling the disk — disk-full is a cluster-wide PANIC, the
limit is a one-query error.
| `PG_VM_POOL_USER` / `PG_VM_POOL_PASSWORD` | `postgres` / unset | probe+bootstrap credentials, and (if set) the required client password |
| `PG_VM_POOL_IDLE_TIMEOUT_SECS` | `900` | stop a VM after this long with no connections; `0` disables |
| `PG_VM_POOL_KEEPALIVE_SCHEMAS` | none | comma-separated schemas exempt from idle reaping |
| `PG_VM_POOL_DATA_DISK_GB` | `4` | persistent per-schema disk size |
| `PG_VM_POOL_READY_TIMEOUT_SECS` | `300` | max wait for VM+Postgres readiness |
| `PG_VM_POOL_CONNECT_TIMEOUT_SECS` | `30` | iroh tunnel handshake cap |
| `PG_VM_POOL_DIRECT_CONNECT` | on | dial guest IP directly; `0` forces the tunnel |
| `PG_VM_POOL_STATE_FILE` | `~/.heyo/pg-vm-pool/registry.tsv` | persisted schema→VM map |
| `PG_VM_POOL_TLS_CERT` / `PG_VM_POOL_TLS_KEY` | unset (TLS off) | PEM cert chain + key; see TLS below |
| `PG_VM_POOL_DASHBOARD_LISTEN` | unset (dashboard off) | HTTP listen address for the admin dashboard; setting it enables the dashboard — see Dashboard below |
| `PG_VM_POOL_DASHBOARD_USER` / `PG_VM_POOL_DASHBOARD_PASSWORD` | unset (no auth) | HTTP Basic auth credentials for the dashboard (must be set together) |
| `PG_VM_POOL_POOLER_LOG` | `/var/log/pg-vm-pool/pg-vm-pool.log` | pooler log file the dashboard tails |
| `PG_VM_POOL_HEYVMD_LOG` | `/var/log/heyvmd/heyvmd.log` | heyvmd log file the dashboard tails |
| `PG_VM_POOL_DASHBOARD_LOG_LINES` | `200` | how many trailing lines the dashboard shows per log |

**Direct connect (default):** when the pooler shares the host with the VMs (the
local-daemon deployment), it dials each VM's Postgres directly at its `guest_ip`
over the host tap and skips the iroh tunnel entirely — no relay dependency,
lower latency, faster bring-up. It falls back to a tunnel automatically if the
daemon reports no `guest_ip`. Set `PG_VM_POOL_DIRECT_CONNECT=0` to force the
tunnel path (e.g. if the pooler ever runs on a different machine than the VMs).

### Managing with supervisord

`deploy/supervisor/pg-vm-pool.conf` runs the release binary under supervisord
and is the single place to manage the pooler's environment:

```sh
cargo build --release
sudo ln -s /home/sam/Projects/pg-fc/deploy/supervisor/pg-vm-pool.conf \
           /etc/supervisor/conf.d/pg-vm-pool.conf
sudo mkdir -p /var/log/pg-vm-pool
sudo supervisorctl reread && sudo supervisorctl update
sudo supervisorctl status pg-vm-pool          # start/stop/restart/tail work too
```

Edit the `environment=` block in the conf to change any `PG_VM_POOL_*` var,
then `supervisorctl reread && supervisorctl update pg-vm-pool` — note a plain
`restart` does **not** reload `environment=`; `update` does. Comma-containing
values (like `KEEPALIVE_SCHEMAS`) must be double-quoted. Logs land in
`/var/log/pg-vm-pool/pg-vm-pool.log`.

### Client auth

The pooler has no client auth gate by default — any client that can reach
`PG_VM_POOL_LISTEN` is proxied straight through to a VM, whatever the VM's
Postgres itself would accept. Set `PG_VM_POOL_PASSWORD` to close that: the
pooler then answers each client's `StartupMessage` with an
`AuthenticationCleartextPassword` challenge and rejects (`28P01`, "password
authentication failed") anyone who doesn't send it back before ever dialing
the backend VM. This is deliberately a separate layer from backend auth — the
VM's own Postgres can (and by default does) stay on `trust`, since gating
access is now the pooler's job.

Because it's cleartext, the password crosses the network unencrypted unless
the connection is also TLS — required reading if `PG_VM_POOL_LISTEN` binds to
anything other than `127.0.0.1` (the pooler logs a startup warning in that
case). Set `PG_VM_POOL_TLS_CERT`/`KEY` alongside it; see TLS below.

### TLS

TLS is **off by default** and fully optional: without it the pooler answers the
Postgres `SSLRequest` with `N` and clients proceed in plaintext exactly as
before (`sslmode=prefer` falls back silently; `sslmode=disable` is unaffected).

To enable, point the pooler at a PEM cert chain + private key:

```sh
PG_VM_POOL_TLS_CERT=/path/fullchain.pem \
PG_VM_POOL_TLS_KEY=/path/privkey.pem \
target/release/pg-vm-pool
```

Both must be set together (setting only one is a startup error). With TLS on,
clients that ask get an encrypted session (`sslmode=require` works) and
plaintext clients are **still accepted** — nothing breaks for existing local
consumers. TLS terminates at the pooler; the pooler→VM hop stays plaintext over
the host-local tap.

The cert files are **hot-reloaded**: the pooler stats them before each
handshake and rebuilds its acceptor when they change, so an external renewer
can rotate certs with no pooler restart. With Let's Encrypt/certbot:

```sh
# one-time issuance (needs public DNS -> this host, port 80 free for the challenge)
sudo certbot certonly --standalone -d pg.example.com

# deploy hook: copy renewed certs somewhere the pooler user can read
sudo tee /etc/letsencrypt/renewal-hooks/deploy/pg-vm-pool.sh >/dev/null <<'EOF'
#!/bin/sh
d=/home/sam/.heyo/pg-vm-pool/tls
mkdir -p "$d"
install -o sam -g sam -m 600 "$RENEWED_LINEAGE/fullchain.pem" "$d/fullchain.pem"
install -o sam -g sam -m 600 "$RENEWED_LINEAGE/privkey.pem"  "$d/privkey.pem"
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/pg-vm-pool.sh
# run the copy once by hand after the first issuance, then renewals are automatic
```

Then set `PG_VM_POOL_TLS_CERT`/`KEY` to those copies (see the commented lines
in the supervisor conf). For clients beyond localhost also set
`PG_VM_POOL_LISTEN=0.0.0.0:6432`, open the firewall, and have clients dial the
certificate's hostname (`sslmode=verify-full host=pg.example.com`).

### Dashboard

An optional server-side-rendered admin dashboard runs **inside the pooler
process** (a background task sharing the live registry), so it can show the
pooler's in-memory session counts alongside the daemon's VM inventory. It's
**off by default** and enabled purely by setting a listen address:

```sh
PG_VM_POOL_DASHBOARD_LISTEN=127.0.0.1:8080 \
PG_VM_POOL_DASHBOARD_USER=admin \
PG_VM_POOL_DASHBOARD_PASSWORD=secret \
target/release/pg-vm-pool
```

What it gives you (browse to the listen address):

- **VM/session overview** (`/`) — every heyvmd sandbox, with power state,
  allocated size (vCPU/RAM), uptime, and live pooler sessions. Pooler-managed
  `pg-<schema>` VMs are grouped first and link to a detail page.
- **Detail page** — full daemon config (size class + resources, image, region,
  guest IP, TTL, status) plus live **database size and backend count**, read
  over the pooler's own warm Postgres connection (a normal query, not a guest
  command).
- **Logs** — tail the pooler log (`/logs/pooler`), the heyvmd log
  (`/logs/heyvmd`), and any VM's in-guest Postgres log (`/logs/vm/<id>`).
- **Controls** — stop / start / reboot / resize any VM from its detail page.
  Note that a pooler-managed VM stopped here auto-restarts on the next client
  connection, and a resize takes effect on the VM's next boot.

The browsable pages (index + detail) perform **no in-guest command execution** —
they read only the daemon inventory and the pooler's own PG pool, so viewing or
refreshing a VM never disturbs it. The one exception is the per-VM Postgres log
page (`/logs/vm/<id>`), which runs `tail` inside the guest and is therefore a
deliberate, explicitly-navigated action rather than part of the detail view.
Every daemon and guest call is timeout-bounded, so one wedged VM can't hang a
page. Access is gated by HTTP **Basic auth** when
`PG_VM_POOL_DASHBOARD_USER`/`PASSWORD` are set (they must be set together, or
startup fails). The dashboard can stop and resize **every** VM on the host, so
prefer a loopback/private `PG_VM_POOL_DASHBOARD_LISTEN`; binding it to a
non-loopback address without Basic auth logs a startup warning. The two log
paths default to the supervisord locations above and are overridable with
`PG_VM_POOL_POOLER_LOG` / `PG_VM_POOL_HEYVMD_LOG`.

### Testing

`examples/e2e.rs` and `examples/e2e_concurrent.rs` are end-to-end tests that
exercise the full stack through a real client connection — not mocks: pooler
routing, daemon VM create/stop/restart, and per-VM persistent disks.

- `e2e.rs` drives one schema through several stop/restart cycles and
  hard-asserts each restart actually comes back healthy (the `/dev/vdb` data
  drive is still attached, and the guest's Postgres port is reachable) before
  checking the rows written earlier survived.
- `e2e_concurrent.rs` runs that same create/write/stop/restart/verify cycle for
  several schemas (default 5) **at the same time**, each with distinct data,
  to prove concurrent VMs don't cross-wire and the pooler restarts them all in
  parallel.

Prereqs: a running pooler (`target/release/pg-vm-pool`, default
`127.0.0.1:6432`) and a running local heyvmd daemon. Then:

```sh
cargo run --release --example e2e
cargo run --release --example e2e_concurrent
```

Useful env vars: `E2E_ROWS`, `E2E_CYCLES` (e2e.rs), `E2E_VMS` (e2e_concurrent.rs),
`E2E_STOP_MODE=cli|sdk` (`cli` reproduces a manual/out-of-band stop via
`heyvm stop`, the default and the path that catches the restart-silently-no-ops
bug; `sdk` is the cooperative stop path), and `E2E_KEEP=1` to keep the test
VM(s) around instead of deleting them at the end. See the doc comments at the
top of each file for the full list.
