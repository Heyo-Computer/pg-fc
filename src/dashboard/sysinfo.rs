//! Per-VM resource usage, read from inside the guest with one bounded command.
//! The command string is a constant — only trusted sandbox ids (from
//! `Sandbox::list`) ever select the target, so there's no shell-injection surface.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use heyo_sdk::{CommandRunOptions, Sandbox};

use crate::vm;

/// Bound on the guest exec itself; the outer wall-clock wait adds a small margin.
const GUEST_TIMEOUT: Duration = Duration::from_secs(3);

/// A point-in-time snapshot of a guest's load, memory, and data-disk usage.
pub struct ResourceUsage {
    pub load1: f64,
    pub nproc: u32,
    pub mem_total_b: u64,
    pub mem_avail_b: u64,
    pub disk_total_b: u64,
    pub disk_used_b: u64,
}

impl ResourceUsage {
    pub fn mem_used_b(&self) -> u64 {
        self.mem_total_b.saturating_sub(self.mem_avail_b)
    }
}

// `printf "%d"` in the awk stages forces integer output — awk's default OFMT
// would render large byte counts in scientific notation and break parsing.
// `df -k` (POSIX, busybox-safe) reports KiB; the awk multiplies to bytes and
// counts columns from the end so a wrapped long device name doesn't shift them.
const PROBE_CMD: &str = "cat /proc/loadavg; echo ===; nproc; echo ===; \
awk '/MemTotal/{t=$2}/MemAvailable/{a=$2}END{printf \"%d %d\", t*1024, a*1024}' /proc/meminfo; echo ===; \
df -k /workspace | tail -1 | awk '{printf \"%d %d\", $(NF-4)*1024, $(NF-3)*1024}'";

/// Connect to a VM by id and read its usage.
pub async fn fetch_by_id(id: &str) -> Result<ResourceUsage> {
    let sandbox =
        Sandbox::connect(id.to_string(), vm::local_opts()).context("connecting to VM")?;
    fetch(&sandbox).await
}

/// Read usage from an already-connected sandbox.
pub async fn fetch(sandbox: &Sandbox) -> Result<ResourceUsage> {
    let opts = CommandRunOptions {
        timeout: Some(GUEST_TIMEOUT),
        ..Default::default()
    };
    let res = tokio::time::timeout(
        GUEST_TIMEOUT + Duration::from_secs(2),
        sandbox.commands().run(PROBE_CMD, opts),
    )
    .await
    .context("guest usage probe timed out")?
    .context("running guest usage probe")?;
    if res.exit_code != 0 {
        bail!("usage probe exit {}: {}", res.exit_code, res.stderr);
    }
    parse_usage(&res.stdout)
}

fn parse_usage(out: &str) -> Result<ResourceUsage> {
    let parts: Vec<&str> = out.split("===").collect();
    if parts.len() < 4 {
        bail!("unexpected usage output: {out:?}");
    }
    let load1 = parts[0]
        .split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let nproc = parts[1].trim().parse().unwrap_or(0);
    let mut mem = parts[2].split_whitespace();
    let mem_total_b = mem.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let mem_avail_b = mem.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let mut disk = parts[3].split_whitespace();
    let disk_total_b = disk.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let disk_used_b = disk.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    Ok(ResourceUsage {
        load1,
        nproc,
        mem_total_b,
        mem_avail_b,
        disk_total_b,
        disk_used_b,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_probe_output() {
        let out = "0.15 0.10 0.05 1/80 1234\n===\n2\n===\n536870912 268435456===\n8589934592 1073741824";
        let u = parse_usage(out).unwrap();
        assert_eq!(u.nproc, 2);
        assert!((u.load1 - 0.15).abs() < 1e-9);
        assert_eq!(u.mem_total_b, 536870912);
        assert_eq!(u.mem_avail_b, 268435456);
        assert_eq!(u.mem_used_b(), 268435456);
        assert_eq!(u.disk_total_b, 8589934592);
        assert_eq!(u.disk_used_b, 1073741824);
    }
}
