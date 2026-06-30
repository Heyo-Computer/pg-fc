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

echo "[init] starting postgres (PID 1)"
# exec so postgres becomes PID 1 and handles SIGTERM/SIGINT shutdown directly.
exec gosu postgres postgres -D "$PGDATA"
