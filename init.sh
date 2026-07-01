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

    # Listen on all interfaces so the host/tunnel can reach it.
    echo "listen_addresses = '*'" >> "$PGDATA/postgresql.conf"
    echo "host all all 0.0.0.0/0 trust" >> "$PGDATA/pg_hba.conf"
fi

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
gosu postgres postgres -D "$PGDATA" &

# heyvm's `wait_for_ready` scans the serial console for this exact marker and
# times out (panicking the create) if it never appears. Postgres binds 5432
# within ~1s and the pooler retries `SELECT 1` (wait_pg_ready), so emitting it
# now (rather than after Postgres is fully up) is safe.
echo "HEYVM_READY"

# Hand PID 1 to a shell on the serial console so the daemon's exec channel works
# (see above) and PID 1 stays alive for the VM's lifetime. Firecracker keeps the
# console open, so this never sees EOF while the VM runs.
exec /bin/sh
