#!/usr/bin/env bash
#
# reclaim-disks.sh — return stranded free space from idle VM data disks.
#
# Each per-schema VM has a sparse `data.ext4` (provisioned at
# PG_VM_POOL_DATA_DISK_GB, mostly holes). When Postgres frees blocks inside the
# guest — recycled WAL, vacuumed heap, dropped temp/tables, a reinitialised
# cluster — ext4 marks them free, but with no TRIM/discard reaching the host the
# blocks are never punched out of the backing file. So a disk ratchets toward its
# full provisioned size and never shrinks, even though the live database is tiny
# (we've seen 1.1 GB of data pinning 51 GB on disk).
#
# This offline-trims every data disk whose VM is NOT currently running: attach a
# loop device (which translates discard into a hole-punch on the backing file),
# recover the journal left dirty by an unclean VM kill, `fstrim`, detach.
#
# SAFETY: a disk a live Firecracker still has open is skipped — read/write
# mounting a running guest's filesystem from the host would corrupt it. The
# check is `fuser` on the backing file, so it holds regardless of how the VM was
# started. Everything else is best-effort per disk: one failure never aborts the
# run and never leaves a loop device or mount behind.
#
# Usage:
#   sudo ./reclaim-disks.sh [RUN_DIR]        # default RUN_DIR: ~/.heyo/run
#   sudo DRY_RUN=1 ./reclaim-disks.sh [DIR]  # report candidates, change nothing
#
set -uo pipefail

RUN_DIR="${1:-${HOME}/.heyo/run}"
DRY_RUN="${DRY_RUN:-0}"

die() { echo "error: $*" >&2; exit 1; }
human() { numfmt --to=iec --suffix=B "${1:-0}" 2>/dev/null || echo "${1:-0}B"; }
# Actual on-disk bytes (allocated blocks), not the sparse apparent size.
allocated() { du -B1 "$1" 2>/dev/null | cut -f1; }

if [ "$(id -u)" != 0 ]; then
    # A dry run only reads, so allow it — but warn: without root the /proc scan
    # can't see other users' open files, so the in-use check may under-report.
    [ "$DRY_RUN" = 1 ] || die "must run as root (needs loop-setup, mount, fstrim)"
    echo "warning: not root — dry-run in-use detection may be incomplete" >&2
fi
[ -d "$RUN_DIR" ] || die "run dir not found: $RUN_DIR"
for tool in losetup mount umount fstrim e2fsck find stat du numfmt; do
    command -v "$tool" >/dev/null || die "missing required tool: $tool"
done

# Point-in-time set of every file held open by any process, keyed by
# device:inode. A live Firecracker keeps its data disk open as a plain fd under
# /proc/<pid>/fd — but Firecracker usually runs under *jailer*, which chroots the
# VM, so the fd's path is relative to that chroot and will NOT equal the host
# path. Matching by path therefore silently misses running VMs and trims them
# (corrupting a disk the guest has mounted RW). device:inode is the same file
# object regardless of chroot, bind mount, or mount namespace, so it is the
# safe identity to compare.
#
# Built once (a single `find | stat` over all fds) rather than re-scanning /proc
# per disk, which is O(disks x fds) and does not finish on a busy host. Root sees
# every process's fds; without it some are unreadable and silently skipped (hence
# the not-root warning above).
#
# Still a snapshot: a VM that *starts* mid-run won't be seen — run during low
# traffic. The blast radius of a miss is one disk.
declare -A OPEN_INODES=()
snapshot_open_files() {
    local key
    while IFS= read -r key; do
        [ -n "$key" ] && OPEN_INODES["$key"]=1
    done < <(find /proc/[0-9]*/fd -maxdepth 1 -type l -exec stat -L -c '%d:%i' {} + 2>/dev/null)
}

# In use if some process holds this exact file (device:inode) open. Fails closed:
# if the disk can't be stat'd we treat it as in use and skip it.
disk_in_use() {
    local key
    key=$(stat -c '%d:%i' "$1" 2>/dev/null) || return 0
    [ -n "${OPEN_INODES[$key]:-}" ]
}

shopt -s nullglob
disks=("$RUN_DIR"/sb-*/data.ext4)
[ "${#disks[@]}" -gt 0 ] || die "no sb-*/data.ext4 disks under $RUN_DIR"

reclaimed=0 trimmed=0 skipped=0 failed=0

# Trim one disk with fully local cleanup. Returns non-zero on any failure but the
# caller keeps going — a single bad disk must not stop the sweep.
trim_one() {
    local disk="$1" loop="" mnt="" rc=0
    # Guard: never touch a disk a running VM has open.
    if disk_in_use "$disk"; then
        echo "skip  (in use)      $disk"
        skipped=$((skipped + 1))
        return 0
    fi

    local before after
    before=$(allocated "$disk")

    if [ "$DRY_RUN" = 1 ]; then
        echo "would-trim         $disk  ($(human "$before"))"
        return 0
    fi

    loop=$(losetup --find --show "$disk" 2>/dev/null) || {
        echo "FAIL  (losetup)    $disk"
        failed=$((failed + 1))
        return 1
    }

    # Recover the journal (VMs are killed uncleanly, so it's usually dirty).
    # -p auto-fixes and exits 1 when it did — expected, not a failure. Only a
    # code >= 4 means the filesystem is still bad; then we don't mount it.
    e2fsck -fp "$loop" >/dev/null 2>&1
    local fsck_rc=$?
    if [ "$fsck_rc" -ge 4 ]; then
        echo "FAIL  (fsck=$fsck_rc)     $disk"
        losetup -d "$loop"
        failed=$((failed + 1))
        return 1
    fi

    mnt=$(mktemp -d)
    if ! mount "$loop" "$mnt" 2>/dev/null; then
        echo "FAIL  (mount)      $disk"
        rmdir "$mnt"
        losetup -d "$loop"
        failed=$((failed + 1))
        return 1
    fi

    fstrim "$mnt" 2>/dev/null || rc=1
    umount "$mnt"
    rmdir "$mnt"
    losetup -d "$loop"

    after=$(allocated "$disk")
    local freed=$((before - after))
    [ "$freed" -lt 0 ] && freed=0
    reclaimed=$((reclaimed + freed))
    trimmed=$((trimmed + 1))
    printf 'trim  %-12s %s\n' "-$(human "$freed")" "$disk"
    return "$rc"
}

echo "reclaim-disks: ${#disks[@]} disk(s) under $RUN_DIR${DRY_RUN:+ (dry-run=$DRY_RUN)}"
snapshot_open_files
for disk in "${disks[@]}"; do
    trim_one "$disk"
done

echo "----"
if [ "$DRY_RUN" = 1 ]; then
    echo "dry-run: $((${#disks[@]} - skipped)) candidate(s), $skipped in use (skipped)"
else
    echo "trimmed $trimmed disk(s), reclaimed $(human "$reclaimed"); $skipped in use, $failed failed"
fi
