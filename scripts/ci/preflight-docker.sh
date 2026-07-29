#!/usr/bin/env bash
# Bring the container venue to the shape a GitHub runner would hand us, before
# any docker-backed test/smoke run.
#
# A GitHub-hosted runner is ephemeral: fresh image, known-free disk, no leftover
# containers or volumes from a previous job. Colima is long-lived, so exited
# containers and orphaned volumes accumulate and the disk drifts down. That
# drift is not cosmetic — a disk that fills mid-run fails deep inside the
# install path and reads as a product bug rather than an environment one.
#
# Default sweep is the state a runner would never have carried in: stopped
# containers, dangling images, unused volumes. The build cache is KEPT by
# default — a runner restores an equivalent cache via actions/cache, so wiping
# it locally would diverge from CI, not converge with it. --deep drops it too.
#
# Usage: preflight-docker.sh [--deep] [--min-free-gb N] [--skip-gate]
set -euo pipefail

# A German (or any comma-decimal) locale makes awk print "76,38" and then parse
# it back as 76, so the free-space gate would silently compare the wrong number.
# Pin the numeric locale for every awk/df in this script.
export LC_ALL=C

usage() {
    cat <<'EOF'
Usage: scripts/ci/preflight-docker.sh [--deep] [--min-free-gb N] [--skip-gate]

Bring the Colima/docker venue to the shape a GitHub runner would hand us,
before any container-backed test/smoke/soak run (test-infrastructure/run.sh
calls this automatically; CBM_SKIP_PREFLIGHT=1 opts out for a quick re-run).

Sweeps stopped containers, dangling images and unused anonymous volumes, then
asserts free space on the filesystem backing the docker data root. The build
cache and NAMED compose volumes are KEPT by default — they are this venue's
analogue of actions/cache, so wiping them would diverge from CI, not converge.

Options:
  --deep            Also drop the build cache, named volumes and every unused
                    image (full cold start).
  --min-free-gb N   Free-space floor (default 14 — the runner's SSD size).
                    Below it the script BLOCKS with exit 1: a disk that fills
                    mid-run fails deep inside the install path and reads as a
                    product bug, so refusing to start is the correct outcome.
  --skip-gate       Sweep and report, but never block (inspection only).
  -h, --help        This text.
EOF
}

DEEP=0
MIN_FREE_GB=14
SKIP_GATE=0
while [ $# -gt 0 ]; do
    case "$1" in
    --deep) DEEP=1 ;;
    --min-free-gb) MIN_FREE_GB="${2:?--min-free-gb needs a value}"; shift ;;
    --skip-gate) SKIP_GATE=1 ;;
    -h | --help) usage; exit 0 ;;
    *) echo "preflight-docker: unknown argument '$1'. Please consult --help." >&2; exit 2 ;;
    esac
    shift
done

if ! docker info >/dev/null 2>&1; then
    echo "preflight-docker: docker is not reachable — start Colima first" >&2
    echo "  (Docker Desktop is broken on this machine; Colima is the runtime)" >&2
    exit 2
fi

free_gb() {
    # Measure the filesystem that actually backs the docker data root, not the
    # VM's `/`: on Colima they are different mounts, and `/` reported 18 GB free
    # while 12.5 GB of images lived elsewhere. The host's own free space is
    # irrelevant to a container that fills the VM's disk.
    local root
    root="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo /var/lib/docker)"
    docker run --rm -v /:/hostroot alpine:3 \
        df -P "/hostroot${root}" 2>/dev/null |
        awk 'NR==2 {printf "%.2f", $4/1048576}'
}

before="$(free_gb || echo 0)"
echo "=== preflight-docker: sweeping runner-unlike residue ==="
# Anonymous volumes only: the NAMED compose volumes are this venue's warm cache
# (the local analogue of actions/cache), so dropping them would diverge from CI
# rather than converge with it. --deep takes them too.
docker container prune -f
docker volume prune -f
docker image prune -f
if [ "$DEEP" -eq 1 ]; then
    echo "--- deep: dropping build cache, named volumes and every unused image ---"
    docker builder prune -af
    docker volume prune -af
    docker image prune -af
fi
after="$(free_gb || echo 0)"
echo "preflight-docker: free-gb-before=${before} free-gb-after=${after}"
docker system df

if [ "$SKIP_GATE" -eq 0 ] &&
    awk -v a="$after" -v m="$MIN_FREE_GB" 'BEGIN {exit !(a < m)}'; then
    echo "BLOCKED: ${after} GB free, need ${MIN_FREE_GB} GB. A GitHub runner never" >&2
    echo "  starts this low, so this run would not be comparable. Re-run with" >&2
    echo "  --deep to reclaim the build cache, or free space on the Colima VM." >&2
    exit 1
fi
