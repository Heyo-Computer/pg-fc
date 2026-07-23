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
# SHRINK=1 additionally *shrinks the filesystem* toward its used size before
# trimming. Legacy disks were formatted across the whole device, so even after
# an fstrim their next growth ratchets straight back toward the full
# provisioned size, and the ~1GB of full-device ext4 metadata stays allocated
# forever. Shrinking retro-fits the thin-provisioning cap the image now applies
# at first format (see init.sh): target = used * 1.25, floored at MIN_FS_MB
# (default 4096, matching init.sh's initial size); the guest's grow watcher
# re-extends the filesystem online if the database needs more. Blocks past the
# new filesystem end are hole-punched out of the backing file directly (fstrim
# can't reach past the fs). The shrunk fs is re-fscked before anything mounts
# it; a disk that fails that check is reported and left unmounted.
#
# Usage:
#   sudo ./reclaim-disks.sh [RUN_DIR]        # default RUN_DIR: ~/.heyo/run
#   sudo DRY_RUN=1 ./reclaim-disks.sh [DIR]  # report candidates, change nothing
#   sudo SHRINK=1 ./reclaim-disks.sh [DIR]   # also shrink filesystems (slower)
#   sudo PRUNE_SWAP=1 ./reclaim-disks.sh [DIR]  # also delete guest swapfiles
#
# PRUNE_SWAP=1 deletes each stopped VM's /swapfile while its fs is mounted.
# Swap contents are dead the moment a VM stops (swap never survives a boot),
# and init.sh recreates the file — sized to the filesystem — on the next boot,
# so this is pure reclaim: the swapfile is fully allocated (fallocate/dd, not
# sparse), up to 2GB per VM on large-RAM fleets, and fstrim alone can never
# touch it because it's a live file.
#
set -uo pipefail

RUN_DIR="${1:-${HOME}/.heyo/run}"
DRY_RUN="${DRY_RUN:-0}"
SHRINK="${SHRINK:-0}"
MIN_FS_MB="${MIN_FS_MB:-4096}"
PRUNE_SWAP="${PRUNE_SWAP:-0}"

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
if [ "$SHRINK" = 1 ]; then
    for tool in dumpe2fs resize2fs fallocate; do
        command -v "$tool" >/dev/null || die "missing required tool for SHRINK=1: $tool"
    done
fi

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
    local key path pid
    while read -r key path; do
        [ -n "$key" ] || continue
        # Remember (one of) the holding PIDs so a skip can say *who* — the
        # difference between "that VM is running" and "something unexpected
        # holds every disk" is the whole diagnosis.
        pid="${path#/proc/}"
        pid="${pid%%/*}"
        OPEN_INODES["$key"]="${OPEN_INODES[$key]:-$pid}"
    done < <(find /proc/[0-9]*/fd -maxdepth 1 -type l -exec stat -L -c '%d:%i %n' {} + 2>/dev/null)
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

reclaimed=0 trimmed=0 skipped=0 failed=0 shrunk=0 dry_reclaimable=0

# SHRINK=1: shrink the (unmounted, freshly fsck'd) filesystem on $1 toward its
# used size. On success sets SHRINK_PUNCH_OFFSET to the new filesystem's end in
# bytes — the caller hole-punches the backing file past that point *after*
# detaching the loop device (never while attached, so no page-cache aliasing on
# the punched range). Returns: 0 = shrunk or nothing to do, 1 = shrink failed
# (fs untouched or restored — trimming may proceed), 2 = post-shrink fsck
# failed (do NOT mount).
SHRINK_PUNCH_OFFSET=""
shrink_fs() {
    local loop="$1" geom bs blocks free used target floor_blk
    SHRINK_PUNCH_OFFSET=""
    geom=$(dumpe2fs -h "$loop" 2>/dev/null) || return 1
    bs=$(awk -F: '/^Block size:/ {gsub(/ /,"",$2); print $2}' <<<"$geom")
    blocks=$(awk -F: '/^Block count:/ {gsub(/ /,"",$2); print $2}' <<<"$geom")
    free=$(awk -F: '/^Free blocks:/ {gsub(/ /,"",$2); print $2}' <<<"$geom")
    { [ -n "$bs" ] && [ -n "$blocks" ] && [ -n "$free" ]; } || return 1
    used=$((blocks - free))
    target=$((used + used / 4))
    floor_blk=$((MIN_FS_MB * 1024 * 1024 / bs))
    [ "$target" -lt "$floor_blk" ] && target=$floor_blk
    # Not worth a slow, block-moving resize unless it retires at least 256MB
    # of future ratchet room.
    [ "$blocks" -le $((target + 268435456 / bs)) ] && return 0
    resize2fs "$loop" "$target" >/dev/null 2>&1 || return 1
    # A shrink relocates data; verify the result before anything mounts it.
    e2fsck -fp "$loop" >/dev/null 2>&1
    [ $? -ge 4 ] && return 2
    SHRINK_PUNCH_OFFSET=$((target * bs))
    return 0
}

# Trim one disk with fully local cleanup. Returns non-zero on any failure but the
# caller keeps going — a single bad disk must not stop the sweep.
trim_one() {
    local disk="$1" loop="" mnt="" rc=0
    # Guard: never touch a disk a running VM has open. Name the holder — an
    # expected skip is a firecracker/jailer PID; anything else (or *every*
    # disk skipped) points at a process unexpectedly pinning the fleet.
    if disk_in_use "$disk"; then
        local key pid comm="?"
        key=$(stat -c '%d:%i' "$disk" 2>/dev/null)
        pid="${OPEN_INODES[${key:-none}]:-?}"
        [ -r "/proc/$pid/comm" ] && comm=$(cat "/proc/$pid/comm" 2>/dev/null)
        echo "skip  (in use by pid $pid/$comm)  $disk"
        skipped=$((skipped + 1))
        return 0
    fi

    local before after
    before=$(allocated "$disk")

    if [ "$DRY_RUN" = 1 ]; then
        # Estimate what a trim would actually free: allocated bytes minus the
        # filesystem's used bytes. Without this, "would-trim" reads as savings
        # when it's only candidacy — an already-lean disk would-trims ~0B.
        local est=""
        if command -v dumpe2fs >/dev/null; then
            local geom bs total_b free_b
            geom=$(dumpe2fs -h "$disk" 2>/dev/null)
            bs=$(awk -F: '/^Block size:/ {gsub(/ /,"",$2); print $2}' <<<"$geom")
            total_b=$(awk -F: '/^Block count:/ {gsub(/ /,"",$2); print $2}' <<<"$geom")
            free_b=$(awk -F: '/^Free blocks:/ {gsub(/ /,"",$2); print $2}' <<<"$geom")
            if [ -n "$bs" ] && [ -n "$total_b" ] && [ -n "$free_b" ]; then
                local gain=$((before - (total_b - free_b) * bs))
                [ "$gain" -lt 0 ] && gain=0
                est="  ~$(human "$gain") reclaimable"
                dry_reclaimable=$((dry_reclaimable + gain))
            fi
        fi
        echo "would-trim$([ "$SHRINK" = 1 ] && echo ' (+shrink)')  $disk  ($(human "$before") allocated$est)"
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

    # Optional filesystem shrink (see header). Runs on the clean, unmounted fs;
    # the hole-punch past the new fs end happens after loop detach, below.
    local punch_off=""
    if [ "$SHRINK" = 1 ]; then
        shrink_fs "$loop"
        case $? in
            0) punch_off="$SHRINK_PUNCH_OFFSET" ;;
            1) echo "note  (shrink failed, trim only)  $disk" ;;
            2)
                echo "FAIL  (fsck after shrink)  $disk"
                losetup -d "$loop"
                failed=$((failed + 1))
                return 1
                ;;
        esac
    fi

    mnt=$(mktemp -d)
    if ! mount "$loop" "$mnt" 2>/dev/null; then
        echo "FAIL  (mount)      $disk"
        rmdir "$mnt"
        losetup -d "$loop"
        failed=$((failed + 1))
        return 1
    fi

    # Drop the dead swapfile before trimming so its blocks get punched too.
    if [ "$PRUNE_SWAP" = 1 ]; then
        rm -f "$mnt/swapfile"
    fi
    fstrim "$mnt" 2>/dev/null || rc=1
    umount "$mnt"
    rmdir "$mnt"
    losetup -d "$loop"

    # With the fs shrunk, everything in the backing file past its end is dead —
    # old metadata/data from the full-size format that fstrim (fs-scoped) can't
    # reach. Punch it out now that the loop device (and its page cache) is gone.
    if [ -n "$punch_off" ]; then
        local file_bytes
        file_bytes=$(stat -c %s "$disk" 2>/dev/null || echo 0)
        if [ "$file_bytes" -gt "$punch_off" ]; then
            fallocate -p -o "$punch_off" -l $((file_bytes - punch_off)) "$disk" 2>/dev/null
        fi
        shrunk=$((shrunk + 1))
    fi

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
    echo "dry-run: $((${#disks[@]} - skipped)) candidate(s), ~$(human "$dry_reclaimable") reclaimable by trim, $skipped in use (skipped)"
else
    shrink_note=""
    [ "$SHRINK" = 1 ] && shrink_note=" ($shrunk filesystem(s) shrunk)"
    echo "trimmed $trimmed disk(s), reclaimed $(human "$reclaimed")$shrink_note; $skipped in use, $failed failed"
fi
