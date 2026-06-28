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
# pg-fc
