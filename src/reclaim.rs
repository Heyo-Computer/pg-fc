//! Automatic reclamation of stranded disk slack on stopped VMs.
//!
//! Firecracker's virtio-blk doesn't pass discard through to the host, so blocks
//! freed inside a guest are never punched out of its sparse `data.ext4`: each
//! disk ratchets toward its provisioned max (`PG_VM_POOL_DATA_DISK_GB`) and
//! stays there, and the machine's real VM capacity becomes
//! `host_disk / provisioned_max` instead of `host_disk / live_data`. The space
//! can only be returned offline — loop-mount a *stopped* VM's disk on the host
//! and `fstrim` it (the loop device does translate discard into hole punches on
//! the backing file), which is what `reclaim-disks.sh` does.
//!
//! This module makes the pooler run that command itself instead of a human:
//! periodically, shortly after the idle reaper stops VMs (so a just-reaped VM's
//! slack comes back within a minute rather than at the next interval), and on
//! demand from the dashboard. The command needs root for loop-setup/mount, so a
//! non-root pooler runs it through a `NOPASSWD` sudoers entry, e.g.
//!
//! ```text
//! PG_VM_POOL_RECLAIM_CMD="sudo -n /opt/pg-vm-pool/reclaim-disks.sh /workbooks/heyvm/run"
//! ```
//!
//! Safety is layered. The script skips any disk a running VM holds open
//! (device:inode match against every open fd on the host) — but it takes that
//! snapshot *once, at pass start*, so a VM booted mid-pass is invisible to it
//! and its filesystem could be fscked/shrunk underneath the running guest,
//! which destroys it. All VM boots go through this process, so the pooler
//! closes that window itself: [`boot_permit`] is the read side of a gate whose
//! write side is held for the full duration of every reclaim run. Boots wait
//! for an in-flight pass (seconds in steady state) instead of corrupting a
//! disk. Runs are additionally single-flighted so the periodic timer, the
//! post-reap trigger, and the dashboard button can't stack overlapping sweeps.

use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tokio::sync::{RwLock, RwLockReadGuard};
use tracing::{debug, error, info, warn};

/// Hard bound on one reclaim run. A run fscks + mounts + trims every stopped
/// disk, so a big fleet legitimately takes minutes — but a wedged mount must
/// not pin the single-flight flag forever. On expiry the child is killed
/// (`kill_on_drop`); the script cleans up per disk, so nothing is left mounted.
const RECLAIM_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Delay between the idle reaper stopping VMs and the follow-up reclaim run.
/// Gives the daemon time to fully tear the Firecracker processes down so their
/// disks no longer show as open — if one is still closing, the script's in-use
/// guard skips it and the periodic run catches it later.
pub const POST_STOP_RECLAIM_DELAY: Duration = Duration::from_secs(30);

/// Delay before the first periodic run after startup — long enough to let the
/// pooler finish coming up and restore any warm VMs, short enough that frequent
/// redeploys can't starve reclamation (see `registry::supervise`).
pub const RECLAIM_FIRST_DELAY: Duration = Duration::from_secs(300);

/// Boot↔reclaim mutual exclusion. Readers are VM boots (start of a stopped VM,
/// power-cycle, dashboard start/reboot); the writer is a reclaim run, held for
/// the run's whole duration. Rationale: the script's in-use scan is a snapshot
/// taken at pass start, so only keeping boots and passes disjoint in time makes
/// "stopped disk" a stable fact for the length of a pass. Freshly *created*
/// disks don't need the permit — they didn't exist when the pass enumerated.
///
/// tokio's RwLock is fair: a waiting pass blocks later boots until it finishes,
/// so boots can stall for up to one pass duration (~seconds in steady state,
/// minutes only during a first full-fleet conversion) — never longer, since
/// new readers can't starve the writer either.
static BOOT_GATE: RwLock<()> = RwLock::const_new(());

/// Take the boot side of the gate: resolves immediately unless a reclaim pass
/// is running, in which case it waits for the pass to finish. Hold the guard
/// across the daemon call that (re)opens a stopped VM's disk — once the VM
/// process holds the disk open, the next pass's in-use scan protects it.
pub async fn boot_permit() -> RwLockReadGuard<'static, ()> {
    BOOT_GATE.read().await
}

/// Runs the configured reclaim command, at most one instance at a time.
pub struct Reclaimer {
    cmd: String,
    running: AtomicBool,
}

impl Reclaimer {
    pub fn new(cmd: String) -> Self {
        Self {
            cmd,
            running: AtomicBool::new(false),
        }
    }

    /// One reclaim run: execute the command via `sh -c`, bounded by
    /// [`RECLAIM_TIMEOUT`], and log its outcome. Returns the number of disks
    /// the script reported trimming (its `trim  -<freed>  <disk>` lines), for
    /// the supervisor's heartbeat. Single-flighted: returns 0 immediately if a
    /// run is already in progress.
    pub async fn run_once(&self) -> usize {
        if self.running.swap(true, Ordering::SeqCst) {
            info!("disk reclaim: a run is already in progress; skipping");
            return 0;
        }
        let _guard = RunningGuard(&self.running);

        // Exclusive with VM boots for the whole run — see BOOT_GATE.
        let waited = Instant::now();
        let _exclusive = BOOT_GATE.write().await;
        if waited.elapsed() > Duration::from_secs(1) {
            info!(
                "disk reclaim: waited {:?} for in-flight VM boots",
                waited.elapsed()
            );
        }

        debug!("disk reclaim: running `{}`", self.cmd);
        let mut command = tokio::process::Command::new("sh");
        command
            .arg("-c")
            .arg(&self.cmd)
            .stdin(Stdio::null())
            .kill_on_drop(true);
        let output = match tokio::time::timeout(RECLAIM_TIMEOUT, command.output()).await {
            Err(_) => {
                error!(
                    "disk reclaim: `{}` did not finish within {RECLAIM_TIMEOUT:?} — killed; \
                     the script cleans up per disk, so nothing is left mounted",
                    self.cmd
                );
                return 0;
            }
            Ok(Err(e)) => {
                error!("disk reclaim: could not launch `{}`: {e}", self.cmd);
                return 0;
            }
            Ok(Ok(out)) => out,
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let trimmed = stdout
            .lines()
            .filter(|l| l.starts_with("trim ") || l.starts_with("would-trim"))
            .count();
        // The script ends with a one-line summary ("trimmed N disk(s),
        // reclaimed X; ..."); surface that instead of the whole per-disk log.
        let summary = stdout
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty() && !l.starts_with("----"))
            .unwrap_or("(no output)");
        // Per-disk FAIL lines name the disks whose fsck failed — without this
        // the summary's "N failed" count is unactionable.
        for line in stdout.lines().filter(|l| l.starts_with("FAIL")) {
            warn!("disk reclaim: {line}");
        }
        if output.status.success() {
            info!("disk reclaim: {summary}");
            if !stderr.trim().is_empty() {
                warn!("disk reclaim stderr: {}", tail(&stderr, 500));
            }
        } else {
            error!(
                "disk reclaim: `{}` exited with {}: {} — stderr: {}",
                self.cmd,
                output.status,
                summary,
                tail(&stderr, 500),
            );
        }
        trimmed
    }

    /// Fire-and-forget run after `delay` — the idle reaper's post-stop trigger.
    /// Quietly does nothing if a run is already in progress (the running sweep
    /// or the next periodic one will pick the freshly stopped disks up).
    pub fn spawn_soon(self: &Arc<Self>, delay: Duration) {
        if self.running.load(Ordering::SeqCst) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            this.run_once().await;
        });
    }

    /// Kick off one run right now, in the background — the dashboard's
    /// "reclaim disk slack" control. Errors if a run is already in progress so
    /// the button gives feedback instead of silently queueing.
    pub fn spawn_now(self: &Arc<Self>) -> Result<()> {
        if self.running.load(Ordering::SeqCst) {
            bail!("a disk reclaim run is already in progress");
        }
        let this = self.clone();
        tokio::spawn(async move {
            let n = this.run_once().await;
            info!("manual disk reclaim finished: trimmed {n} disk(s)");
        });
        Ok(())
    }
}

/// Clears the single-flight flag on drop, so an early return (timeout, launch
/// failure) or panic can't leave the reclaimer permanently "running".
struct RunningGuard<'a>(&'a AtomicBool);

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Last `n` bytes of `s` (on a char boundary), for bounded error logs.
fn tail(s: &str, n: usize) -> &str {
    let s = s.trim();
    let mut start = s.len().saturating_sub(n);
    while start > 0 && !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_once_counts_trimmed_disks() {
        let r = Reclaimer::new(
            "printf 'reclaim-disks: 3 disk(s)\ntrim  -1.0GB  /a\ntrim  -2.0GB  /b\n\
             skip  (in use)  /c\n----\ntrimmed 2 disk(s), reclaimed 3.0GB\n'"
                .to_string(),
        );
        assert_eq!(r.run_once().await, 2);
    }

    #[tokio::test]
    async fn run_once_single_flights() {
        let r = Reclaimer::new("echo 'trim  -1.0GB  /a'".to_string());
        r.running.store(true, Ordering::SeqCst);
        // A concurrent caller must bail out immediately, not run the command.
        assert_eq!(r.run_once().await, 0);
        // And it must not have cleared the original holder's flag.
        assert!(r.running.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn run_once_reports_failure_as_zero() {
        let r = Reclaimer::new("echo boom >&2; exit 3".to_string());
        assert_eq!(r.run_once().await, 0);
        // Flag released for the next run.
        assert!(!r.running.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn spawn_now_rejects_while_running() {
        let r = Arc::new(Reclaimer::new("true".to_string()));
        r.running.store(true, Ordering::SeqCst);
        assert!(r.spawn_now().is_err());
    }

    #[tokio::test]
    async fn boots_wait_for_a_running_reclaim_pass() {
        let r = Arc::new(Reclaimer::new("sleep 0.5".to_string()));
        let run = tokio::spawn({
            let r = r.clone();
            async move { r.run_once().await }
        });
        // Let the run acquire the write side of the gate.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let waited = Instant::now();
        let _permit = boot_permit().await;
        // The permit must not have resolved while the pass was still running.
        // (One-sided: other tests share the global gate and can only add time.)
        assert!(
            waited.elapsed() >= Duration::from_millis(200),
            "boot permit resolved after {:?} — during the reclaim pass",
            waited.elapsed()
        );
        run.await.unwrap();
    }

    #[test]
    fn tail_respects_char_boundaries() {
        assert_eq!(tail("héllo", 3), "llo");
        assert_eq!(tail("héllo", 100), "héllo");
    }
}
