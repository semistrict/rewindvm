#!/usr/bin/env bash
# Build an EROFS root image containing unmodified official FoundationDB 8.0.1
# (`fdbserver`, `fdbcli`, `libfdb_c.so`) plus `python3` for Rewind VM clusters.
#
# Usage:
#   examples/cluster/foundationdb/build_image.sh [output.erofs]
#
# Defaults to `target/<arch>-erofs/fdb.erofs` (matching the host architecture:
# `arm64` on Apple Silicon / aarch64 Linux, `x86_64` on Intel/AMD Linux).
set -euo pipefail

FDB_VERSION="${FDB_VERSION:-8.0.1}"
REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"

HOST_ARCH="$(uname -m)"
case "${ARCH:-$HOST_ARCH}" in
  arm64|aarch64)
    DOCKER_PLATFORM="linux/arm64"
    FDB_ARCH="aarch64"
    OUT_SUBDIR="arm64-erofs"
    ;;
  x86_64|amd64)
    DOCKER_PLATFORM="linux/amd64"
    FDB_ARCH="x86_64"
    OUT_SUBDIR="x86_64-erofs"
    ;;
  *)
    echo "Unsupported architecture: ${ARCH:-$HOST_ARCH}" >&2
    exit 1
    ;;
esac

OUT_PATH="${1:-${REPO_ROOT}/target/${OUT_SUBDIR}/fdb.erofs}"
OUT_DIR="$(dirname "${OUT_PATH}")"
OUT_NAME="$(basename "${OUT_PATH}")"
mkdir -p "${OUT_DIR}"

TMP_DL="$(mktemp -d)"
trap 'rm -rf "${TMP_DL}"' EXIT

BASE_URL="https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}"
echo "Downloading FoundationDB ${FDB_VERSION} (${FDB_ARCH}) release binaries..."
for asset in "fdbserver.${FDB_ARCH}" "fdbcli.${FDB_ARCH}" "libfdb_c.${FDB_ARCH}.so"; do
  curl -fsSL -o "${TMP_DL}/${asset}" "${BASE_URL}/${asset}"
done
chmod +x "${TMP_DL}/fdbserver.${FDB_ARCH}" "${TMP_DL}/fdbcli.${FDB_ARCH}"

echo "Packing ${OUT_PATH} via Docker (${DOCKER_PLATFORM})..."
docker run --rm --platform "${DOCKER_PLATFORM}" \
  -v "${TMP_DL}:/fdb:ro" \
  -v "${OUT_DIR}:/out" \
  -e "FDB_ARCH=${FDB_ARCH}" \
  -e "OUT_NAME=${OUT_NAME}" \
  debian:bookworm-slim bash -c '
set -euo pipefail
apt-get update -qq
apt-get install -y -qq --no-install-recommends python3 ca-certificates iproute2 procps erofs-utils >/dev/null
mkdir -p /rootfs
cp -a /bin /etc /lib /sbin /usr /rootfs/
mkdir -p /rootfs/dev /rootfs/proc /rootfs/sys /rootfs/tmp \
         /rootfs/var/lib/fdb /rootfs/var/log/fdb /rootfs/build /rootfs/root /rootfs/run
cp "/fdb/fdbserver.${FDB_ARCH}" /rootfs/usr/local/bin/fdbserver
cp "/fdb/fdbcli.${FDB_ARCH}" /rootfs/usr/local/bin/fdbcli
cp "/fdb/libfdb_c.${FDB_ARCH}.so" /rootfs/usr/lib/libfdb_c.so
chmod +x /rootfs/usr/local/bin/fdbserver /rootfs/usr/local/bin/fdbcli
chroot /rootfs /usr/local/bin/fdbserver --version
chroot /rootfs /usr/local/bin/fdbcli --version
mkfs.erofs -zlz4 "/out/${OUT_NAME}" /rootfs
ls -lh "/out/${OUT_NAME}"
'

echo "Built ${OUT_PATH}"
