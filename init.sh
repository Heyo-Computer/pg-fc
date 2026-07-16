#!/bin/sh
# PID 1 inside the Firecracker microVM.
#
# Responsibilities, in order:
#   1. Mount the kernel pseudo-filesystems a normal init would set up.
#   2. Mount the persistent data volume at /workspace (formatting it on first
#      boot if it has no filesystem yet).
#   3. Initialize the Postgres cluster in $PGDATA if it doesn't exist.
#   4. exec postgres so it inherits PID 1 and receives the VM's signals
#      (a clean shutdown on SIGTERM, reaping handled by postgres itself).
#
# Anything that exits PID 1 panics the kernel, so failures here are fatal by
# design — we let `set -e` halt and the boot log shows where.

set -e

# The kernel hands PID 1 a minimal PATH, and Docker's `ENV PATH` does NOT apply
# on the Firecracker boot path (no container runtime sets it up). The versioned
# Postgres binaries live in /usr/lib/postgresql/<major>/bin, so without this
# `gosu postgres initdb` fails with "initdb: executable file not found in $PATH"
# and PID 1 exits → kernel panic. Establish a sane base PATH and prepend every
# installed major's bin dir (glob stays literal and is skipped if none match).
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
for pgbin in /usr/lib/postgresql/*/bin; do
    [ -d "$pgbin" ] && PATH="$pgbin:$PATH"
done

# The block device Firecracker exposes for the data volume. Override via the
# kernel cmdline (e.g. `pgdata_dev=/dev/vdc`) when the drive ordering differs.
DATA_DEV="${pgdata_dev:-/dev/vdb}"
WORKSPACE=/workspace
PGDATA="${PGDATA:-/workspace/pgdata}"

echo "[init] mounting pseudo-filesystems"
mount -t proc     proc     /proc      2>/dev/null || true
mount -t sysfs    sysfs    /sys       2>/dev/null || true
mount -t devtmpfs devtmpfs /dev       2>/dev/null || true
mount -t tmpfs    tmpfs    /run       2>/dev/null || true
mount -t tmpfs    tmpfs    /tmp       2>/dev/null || true

# Pull cmdline overrides (pgdata_dev=...) into the environment if Firecracker
# passed them as boot args. /proc must be mounted first (done above).
for arg in $(cat /proc/cmdline); do
    case "$arg" in
        pgdata_dev=*) DATA_DEV="${arg#pgdata_dev=}" ;;
    esac
done

if [ -b "$DATA_DEV" ]; then
    echo "[init] data volume $DATA_DEV present"
    # Format on first boot only: blkid prints nothing for an unformatted device.
    if ! blkid "$DATA_DEV" >/dev/null 2>&1; then
        echo "[init] $DATA_DEV is blank, creating ext4 filesystem"
        mkfs.ext4 -q -L pgdata "$DATA_DEV"
    fi
    echo "[init] mounting $DATA_DEV at $WORKSPACE"
    mount "$DATA_DEV" "$WORKSPACE"
else
    # No dedicated volume attached — fall back to the rootfs so the VM still
    # boots (useful for smoke tests). Data won't persist across reboots.
    echo "[init] WARNING: $DATA_DEV not found, using rootfs for $WORKSPACE (non-persistent)"
fi

# --- swap + overcommit: survive ingest memory spikes -------------------------
# Bulk loads (~1GB COPY + sorted INSERT) can transiently outgrow a small VM's
# RAM. Without swap the kernel OOM killer SIGKILLs a backend and takes the
# whole cluster through crash-restart — the "postgres died inside a live VM"
# case. Two-part fix, applied only when the persistent disk is present:
#   1. A swapfile on the data disk as an emergency spillway (sized to RAM,
#      capped by disk headroom): spikes page out and slow down instead of
#      killing the server.
#   2. vm.overcommit_memory=2 (the setting the Postgres docs recommend for
#      dedicated hosts): allocations beyond swap + 90% RAM *fail*, so the one
#      offending query gets a clean "out of memory" error instead of the OOM
#      killer choosing a victim.
if [ -b "$DATA_DEV" ]; then
    SWAPFILE="$WORKSPACE/swapfile"
    if [ ! -f "$SWAPFILE" ]; then
        mem_mb=$(awk '/^MemTotal:/ {print int($2/1024)}' /proc/meminfo)
        avail_mb=$(df -Pm "$WORKSPACE" | awk 'NR==2 {print $4}')
        swap_mb=$mem_mb
        [ "$swap_mb" -gt 2048 ] && swap_mb=2048
        # Never let swap eat more than an eighth of the disk, and only create
        # it with 2x headroom so it can't crowd out the data it exists to save.
        disk_eighth=$(($(blockdev --getsize64 "$DATA_DEV") / 1024 / 1024 / 8))
        [ "$swap_mb" -gt "$disk_eighth" ] && swap_mb="$disk_eighth"
        if [ "$avail_mb" -gt $((swap_mb * 2)) ]; then
            echo "[init] creating ${swap_mb}MB swapfile at $SWAPFILE"
            if ! fallocate -l "${swap_mb}M" "$SWAPFILE" 2>/dev/null; then
                dd if=/dev/zero of="$SWAPFILE" bs=1M count="$swap_mb" 2>/dev/null
            fi
            chmod 600 "$SWAPFILE"
            mkswap "$SWAPFILE" >/dev/null
        else
            echo "[init] WARNING: not enough free disk for a ${swap_mb}MB swapfile, skipping"
        fi
    fi
    if [ -f "$SWAPFILE" ]; then
        swapon "$SWAPFILE" 2>/dev/null || echo "[init] WARNING: swapon $SWAPFILE failed"
    fi
    # Strict overcommit only alongside swap: ratio first, then the mode switch.
    # Swappiness 10 keeps the swapfile as a pressure valve, not a working set.
    if swapon --show 2>/dev/null | grep -q .; then
        echo 90 > /proc/sys/vm/overcommit_ratio   2>/dev/null || true
        echo 2  > /proc/sys/vm/overcommit_memory  2>/dev/null || true
        echo 10 > /proc/sys/vm/swappiness         2>/dev/null || true
    fi
fi

mkdir -p "$PGDATA"
chown -R postgres:postgres "$WORKSPACE"
chmod 700 "$PGDATA"

# Initialize the cluster the first time this volume is used.
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "[init] initializing new Postgres cluster in $PGDATA"
    # `trust` host auth: initdb never sets a superuser password, so scram over
    # TCP would lock every client out. The microVM isn't directly reachable —
    # the only path in is the per-schema iroh tunnel the pooler opens, which is
    # the real security boundary here. Set a password and switch this back to
    # scram-sha-256 for any deployment where the VM's 5432 is broadly reachable.
    gosu postgres initdb --pgdata="$PGDATA" --encoding=UTF8 --auth-host=trust
fi

# Reconcile the network-facing config on EVERY boot, not just first init. Two
# reasons this must be idempotent and self-healing:
#   1. The pooler stops VMs with an unclean kill (no clean Postgres shutdown).
#      ext4 delayed allocation means a first-boot append to postgresql.conf that
#      was never fsync'd can be left as NUL bytes on disk after that kill, and
#      Postgres then refuses to start ("syntax error ... near token"). Stripping
#      NULs here repairs a config the previous boot corrupted.
#   2. A config lost to corruption (or an image whose base conf lacks these)
#      is restored rather than leaving the VM unreachable.
# Postgres data itself is WAL/fsync-protected; only these plain config appends
# are at risk, so healing them is enough. `sync` at the end makes the repaired
# files durable before we hand control to Postgres.
heal_line() {
    # heal_line <file> <match-prefix> <full-line>
    # Drop any NUL bytes, then ensure exactly the desired directive is present.
    file="$1"; prefix="$2"; line="$3"
    if [ -f "$file" ]; then
        tr -d '\000' < "$file" > "$file.heal" && mv "$file.heal" "$file"
    fi
    if ! grep -q "^$prefix" "$file" 2>/dev/null; then
        echo "$line" >> "$file"
    fi
}
# Listen on all interfaces so the host/tunnel can reach it.
heal_line "$PGDATA/postgresql.conf" "listen_addresses" "listen_addresses = '*'"
heal_line "$PGDATA/pg_hba.conf" "host all all 0.0.0.0/0 trust" "host all all 0.0.0.0/0 trust"

# --- resource-scaled tuning -------------------------------------------------
# Postgres is the only tenant of this VM, so size it from what the guest
# actually has rather than initdb's one-size-fits-nothing defaults. Reading
# RAM/CPUs/disk at boot (instead of baking values into the image) means one
# image serves every PG_VM_POOL_SIZE_CLASS, and a VM whose size class changes
# across a stop/start picks up correct values on the next boot — which is why
# this file is REGENERATED every boot, not healed: stale numbers from a
# previous size are overwritten, and NUL corruption from an unclean kill
# (see above) fixes itself the same way. Only the include directive in
# postgresql.conf needs heal_line.
mem_mb=$(awk '/^MemTotal:/ {print int($2/1024)}' /proc/meminfo)
cpus=$(nproc 2>/dev/null || echo 1)

# Workload (Quickstore): single app, bulk COPY loads into temp tables, jsonb
# analytics + point lookups, datasets up to ~1GB, no replication, and the
# pooler already stops VMs with an unclean kill (crash-safe by design).
shared_buffers_mb=$((mem_mb / 4))
effective_cache_mb=$((mem_mb * 3 / 4))
# Sorts/aggregations benefit from generous work_mem; concurrency is low (one
# app through the pooler), but cap it so many sessions can't OOM a big VM.
# Sized at mem/32: Quickstore's count/validation queries lean on hashed
# subplans (per-row NOT EXISTS anti-joins collapse into one in-memory hash
# only when the ref table's hash fits in work_mem × hash_mem_multiplier) —
# too small and the planner silently degrades to per-row rescans.
work_mem_mb=$((mem_mb / 32)); [ "$work_mem_mb" -gt 256 ] && work_mem_mb=256
[ "$work_mem_mb" -lt 4 ] && work_mem_mb=4
# Index builds / VACUUM after bulk loads.
maint_mem_mb=$((mem_mb / 8)); [ "$maint_mem_mb" -gt 1024 ] && maint_mem_mb=1024
# COPY ingest lands in ON COMMIT DROP temp tables first (temp_buffers, not
# shared_buffers).
temp_buffers_mb=$((mem_mb / 32)); [ "$temp_buffers_mb" -gt 128 ] && temp_buffers_mb=128
# Parallel query only helps with >1 vCPU; on 1 it just context-switches.
# cpus-1 workers + the leader saturates the VM for one analytics query at a
# time — the right trade for single-tenant count/scan bursts.
gather_workers=$((cpus - 1))
[ "$gather_workers" -gt 4 ] && gather_workers=4

# --- connection budget vs. per-backend memory -------------------------------
# max_connections was previously left at initdb's 100 for every size class,
# while the knobs above scale with RAM: going micro→large takes work_mem 4MB→
# 256MB and temp_buffers 16MB→128MB with the connection ceiling unmoved. The
# per-backend appetite grows ~64x, the concurrency bound doesn't, and nothing
# reconciles the two.
#
# That gap is load-bearing here because of the strict overcommit set above:
# vm.overcommit_memory=2 makes the kernel refuse any allocation past
# CommitLimit = swap + overcommit_ratio% of RAM, and CommitLimit is charged
# against *committed* address space, not RSS. So the failure mode is an
# allocation/fork error (and a client connect refused with no protocol reply)
# while `free`/metrics still show RAM to spare — a VM that looks idle right up
# until it stops accepting connections.
#
# Reconcile them: pick a concurrency ceiling, then size the per-backend knobs
# to fit inside what CommitLimit can actually honor at that ceiling.
os_reserve_mb=$((mem_mb / 16)); [ "$os_reserve_mb" -lt 256 ] && os_reserve_mb=256
backend_budget_mb=$((mem_mb - shared_buffers_mb - os_reserve_mb))
[ "$backend_budget_mb" -lt 64 ] && backend_budget_mb=64
# Start from what the vCPU count can usefully service...
max_conns=$((cpus * 25))
[ "$max_conns" -lt 100 ] && max_conns=100
[ "$max_conns" -gt 200 ] && max_conns=200
# ...then refuse to advertise more than the budget can actually honor. A
# backend needs ~10MB of process overhead before any tunable, plus the 8MB
# temp_buffers floor below. Advertising connections past that point doesn't buy
# concurrency, it just moves the failure: the kernel refuses an allocation deep
# inside a transaction instead of Postgres cleanly refusing the connection at
# the door. Prefer the clean refusal — that's what max_connections is for.
affordable=$((backend_budget_mb / 18))
[ "$max_conns" -gt "$affordable" ] && max_conns=$affordable
[ "$max_conns" -lt 25 ] && max_conns=25
per_conn_mb=$((backend_budget_mb / max_conns))
[ "$per_conn_mb" -lt 12 ] && per_conn_mb=12
# Split the allowance. temp_buffers is charged first and is *sticky* — once a
# session touches a temp table its temp_buffers stay allocated until the
# session ends, so with ON COMMIT DROP ingest it is committed per backend for
# real. work_mem is transient (allocated as a sort/hash grows), so it takes
# what's left rather than being budgeted at full worst-case concurrency.
tb_cap=$((per_conn_mb / 2))
[ "$temp_buffers_mb" -gt "$tb_cap" ] && temp_buffers_mb=$tb_cap
[ "$temp_buffers_mb" -lt 8 ] && temp_buffers_mb=8
wm_cap=$((per_conn_mb - temp_buffers_mb))
[ "$work_mem_mb" -gt "$wm_cap" ] && work_mem_mb=$wm_cap
[ "$work_mem_mb" -lt 4 ] && work_mem_mb=4
# Autovacuum workers inherit maintenance_work_mem when autovacuum_work_mem is
# left at -1: 3 workers x 1GB on a large VM, all against the same CommitLimit.
# Give them their own, smaller allowance and leave maintenance_work_mem for
# the foreground index builds it was sized for.
autovac_mem_mb=$((maint_mem_mb / 4)); [ "$autovac_mem_mb" -gt 256 ] && autovac_mem_mb=256
[ "$autovac_mem_mb" -lt 16 ] && autovac_mem_mb=16

# WAL budget. Disk-full is the one crash this VM cannot recover from on its
# own: ENOSPC on a WAL write is a PANIC, and the end-of-recovery checkpoint
# needs WAL space too, so a full disk means a crash loop until someone grows
# the volume. Peak usage is a Quickstore bulk import, where the COPY temp
# table (~1x dataset), the destination sheet table (~1x), WAL, and the
# swapfile all share this disk at once. WAL gets disk/8 — a soft cap that
# only overshoots when checkpoints lag — leaving ~2x-dataset headroom plus
# swap (also capped at disk/8, above) on the default 4GB volume. The 512MB
# floor trades checkpoint frequency for safety on tiny disks: worst case is
# more checkpoints during a big load, never a WAL-full PANIC.
max_wal_mb=512
temp_limit_mb=1024
if [ -b "$DATA_DEV" ]; then
    disk_mb=$(($(blockdev --getsize64 "$DATA_DEV") / 1024 / 1024))
    max_wal_mb=$((disk_mb / 8))
    [ "$max_wal_mb" -lt 512 ] && max_wal_mb=512
    [ "$max_wal_mb" -gt 4096 ] && max_wal_mb=4096
    # Cap per-process spill files: a runaway sort filling the disk is a PANIC
    # (whole-cluster crash), while hitting this limit only errors that query.
    # Temp *tables* don't count against this limit — they're ordinary relation
    # files, budgeted in the arithmetic above.
    temp_limit_mb=$((disk_mb / 4))
fi

cat > "$PGDATA/heyvm-tuning.conf" <<EOF
# Generated by init.sh on every boot from live VM resources — do not edit
# (changes are overwritten; put manual overrides in postgresql.conf, which
# is read after this include). Sized for ${mem_mb}MB RAM, ${cpus} vCPU.
shared_buffers = ${shared_buffers_mb}MB
effective_cache_size = ${effective_cache_mb}MB
work_mem = ${work_mem_mb}MB
maintenance_work_mem = ${maint_mem_mb}MB
temp_buffers = ${temp_buffers_mb}MB

# Concurrency ceiling the per-backend knobs above were sized against
# (${backend_budget_mb}MB of non-shared_buffers headroom / ${max_conns} connections =
# ${per_conn_mb}MB each). Raising this without re-deriving work_mem/temp_buffers
# re-opens the strict-overcommit failure it exists to bound.
max_connections = ${max_conns}
# The pooler's liveness probe connects as a superuser; these slots keep it
# answerable even when the app has taken every ordinary connection, so a
# saturated VM reports "too many clients" to the client rather than going dark
# to the pooler and being misread as dead.
superuser_reserved_connections = 5
max_wal_size = ${max_wal_mb}MB
temp_file_limit = ${temp_limit_mb}MB
max_parallel_workers = ${cpus}
max_parallel_workers_per_gather = ${gather_workers}

# Single-tenant, no replicas: minimal WAL (+ compression — jsonb squeezes
# well) cuts bulk-load I/O substantially.
wal_level = minimal
max_wal_senders = 0
wal_compression = lz4

# The pooler stop-kills this VM anyway, so commits already ride on WAL crash
# recovery; async commit trades <1s of confirmed-commit durability (never
# integrity) for a large ingest speedup — the Quickstore bargain. The pooler
# issues a CHECKPOINT before each idle stop, so the routine reap path loses
# nothing; only a genuine mid-flight crash can drop that sub-second window.
synchronous_commit = off

# Virtio SSD-backed storage: random reads cost ~sequential, deep readahead ok.
random_page_cost = 1.1
effective_io_concurrency = 200

# Datasets ≤1GB and short mixed queries: JIT compile overhead hurts more
# than it helps at this scale.
jit = off

# Reclaim sessions abandoned mid-transaction. A client that opens a
# transaction and then goes away (crashed importer, dropped socket the server
# hasn't noticed) holds its connection slot *and* every lock it took until the
# server times it out — which is how a handful of orphans turns into "too many
# clients already" plus lock waits on tables nothing is actively writing.
# Generous enough that no legitimate import trips it; this only ever fires on
# a session that is idle *inside* a transaction, never on a running query.
idle_in_transaction_session_timeout = 5min

# Lock waits and deadlocks here are almost always two client-side operations
# racing on the same table, and the deadlock report alone names only the two
# statements at the moment of detection. Logging the waits gives the blocked/
# blocking pairs leading up to it, which is what identifies the racing callers.
log_lock_waits = on

# Counts/validation scans run right after bulk loads. Stock autovacuum can
# lag a fresh 1GB import by minutes, during which the planner works from
# stale (or no) stats — mis-planning the count queries' anti-joins — and the
# unset visibility map blocks index-only count(*) scans. Single tenant, so
# aggressive thresholds cost little and keep the first post-import count as
# fast as the tenth.
autovacuum_work_mem = ${autovac_mem_mb}MB
autovacuum_naptime = 15s
autovacuum_vacuum_scale_factor = 0.05
autovacuum_vacuum_insert_scale_factor = 0.05
autovacuum_analyze_scale_factor = 0.02

# Persist server logs under PGDATA (= the data disk), so crash forensics
# survive the reboot that follows a crash — stderr only reaches the serial
# console, and the VM's dmesg dies with each power-cycle. %a gives a
# seven-day ring of weekday files; truncate-on-rotation caps disk use.
logging_collector = on
log_filename = 'postgresql-%a.log'
log_rotation_age = 1d
log_rotation_size = 0
log_truncate_on_rotation = on
EOF
chown postgres:postgres "$PGDATA/heyvm-tuning.conf"
# Include must precede nothing in particular — later lines in postgresql.conf
# (e.g. manual ALTER SYSTEM/edits) still win, since last setting read wins.
heal_line "$PGDATA/postgresql.conf" "include = 'heyvm-tuning.conf'" "include = 'heyvm-tuning.conf'"

chown postgres:postgres "$PGDATA/postgresql.conf" "$PGDATA/pg_hba.conf" 2>/dev/null || true
sync

# Postgres opens its unix socket (and lock file) in /var/run/postgresql, which
# is a symlink to the /run tmpfs we mounted above — so the directory the package
# ships is gone and must be recreated each boot, owned by postgres.
mkdir -p /var/run/postgresql
chown postgres:postgres /var/run/postgresql

echo "[init] starting postgres"
# Run Postgres as a background child rather than `exec`-ing it as PID 1. The
# heyvm daemon drives an exec channel over the serial console (ttyS0): it writes
# `echo START; (cmd); echo END $?` to the console and waits for a *shell* to run
# it. It uses this for lifecycle ops like /etc/hosts injection. If Postgres owns
# the console (exec postgres), nothing services that channel, so every daemon
# exec blocks ~30s then fails over to SSH (also absent) — adding tens of seconds
# to each VM create, and worse as sibling VMs multiply. Keeping a shell on the
# console (below) makes creates fast.
#
# Trade-off: Postgres no longer receives the VM's SIGTERM directly. That's fine
# here — Postgres is crash-safe (WAL replay on next boot) and the data dir is
# currently ephemeral. If clean shutdown matters later, trap TERM in this shell
# and forward it to $PG_PID.
# OOM-killer insurance (matters if the strict-overcommit setup above was
# skipped or still trips): shield the postmaster at -900 so the killer never
# takes out the whole cluster. The score must be lowered *before* dropping
# root (unprivileged processes can only raise it), and children inherit it —
# PG_OOM_ADJUST_FILE is Postgres' built-in reset: the postmaster writes 0 into
# each backend's oom_score_adj at fork, so a runaway backend dies before the
# postmaster (a backend kill is a recoverable restart; losing PID-postmaster
# means our pooler-side power-cycle).
export PG_OOM_ADJUST_FILE=/proc/self/oom_score_adj
export PG_OOM_ADJUST_VALUE=0
sh -c "echo -900 > /proc/self/oom_score_adj 2>/dev/null || true; exec gosu postgres postgres -D '$PGDATA'" &

# heyvm's `wait_for_ready` scans the serial console for this exact marker and
# times out (panicking the create) if it never appears. Postgres binds 5432
# within ~1s and the pooler retries `SELECT 1` (wait_pg_ready), so emitting it
# now (rather than after Postgres is fully up) is safe.
echo "HEYVM_READY"

# Hand PID 1 to a shell on the serial console so the daemon's exec channel works
# (see above) and PID 1 stays alive for the VM's lifetime. Firecracker keeps the
# console open, so this never sees EOF while the VM runs.
exec /bin/sh
