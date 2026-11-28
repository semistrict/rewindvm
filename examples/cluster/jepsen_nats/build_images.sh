#!/usr/bin/env bash
# Build the two EROFS guest images required by `examples/cluster/jepsen_nats.py`:
#   1. `target/<arch>-erofs/nats-2.12.1.erofs`  — unmodified official NATS 2.12.1
#   2. `target/<arch>-erofs/jepsen-nats.erofs`  — OpenJDK 21 + pre-built AOT uberjar
#                                                 (jepsen 0.3.10 + jnats 2.24.1 + jepsen.rewind)
#
# Usage:
#   examples/cluster/jepsen_nats/build_images.sh
set -euo pipefail

NATS_VERSION="${NATS_VERSION:-2.12.1}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

HOST_ARCH="$(uname -m)"
case "${ARCH:-$HOST_ARCH}" in
  arm64|aarch64)
    DOCKER_PLATFORM="linux/arm64"
    NATS_ARCH="arm64"
    OUT_SUBDIR="arm64-erofs"
    ;;
  x86_64|amd64)
    DOCKER_PLATFORM="linux/amd64"
    NATS_ARCH="amd64"
    OUT_SUBDIR="x86_64-erofs"
    ;;
  *)
    echo "Unsupported architecture: ${ARCH:-$HOST_ARCH}" >&2
    exit 1
    ;;
esac

OUT_DIR="${REPO_ROOT}/target/${OUT_SUBDIR}"
mkdir -p "${OUT_DIR}"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

# 1. Download unmodified official NATS 2.12.1 server binary and pack nats-2.12.1.erofs
if [ "${REBUILD_ALL:-0}" = "1" ] || [ ! -f "${OUT_DIR}/nats-${NATS_VERSION}.erofs" ]; then
  NATS_TGZ="nats-server-v${NATS_VERSION}-linux-${NATS_ARCH}.tar.gz"
  NATS_URL="https://github.com/nats-io/nats-server/releases/download/v${NATS_VERSION}/${NATS_TGZ}"
  echo "Downloading NATS ${NATS_VERSION} (${NATS_ARCH})..."
  curl -fsSL -o "${TMP_DIR}/${NATS_TGZ}" "${NATS_URL}"
  tar -xzf "${TMP_DIR}/${NATS_TGZ}" -C "${TMP_DIR}"
  NATS_BIN="$(find "${TMP_DIR}" -name nats-server -type f | head -n 1)"
  cp "${NATS_BIN}" "${TMP_DIR}/nats-server"
  chmod +x "${TMP_DIR}/nats-server"

  echo "Packing ${OUT_DIR}/nats-${NATS_VERSION}.erofs via Docker (${DOCKER_PLATFORM})..."
  docker run --rm --platform "${DOCKER_PLATFORM}" \
    -v "${TMP_DIR}:/dl:ro" \
    -v "${OUT_DIR}:/out" \
    -e "NATS_VERSION=${NATS_VERSION}" \
    debian:bookworm-slim bash -c '
set -euo pipefail
apt-get update -qq
apt-get install -y -qq --no-install-recommends ca-certificates iproute2 procps erofs-utils >/dev/null
mkdir -p /rootfs
cp -a /bin /etc /lib /sbin /usr /rootfs/
mkdir -p /rootfs/dev /rootfs/proc /rootfs/sys /rootfs/tmp /rootfs/build /rootfs/root /rootfs/run
cp /dl/nats-server /rootfs/usr/local/bin/nats-server
chmod +x /rootfs/usr/local/bin/nats-server
chroot /rootfs /usr/local/bin/nats-server --version
mkfs.erofs -zlz4 "/out/nats-${NATS_VERSION}.erofs" /rootfs
ls -lh "/out/nats-${NATS_VERSION}.erofs"
'
else
  echo "Reusing existing ${OUT_DIR}/nats-${NATS_VERSION}.erofs (set REBUILD_ALL=1 to rebuild)"
fi

# 2. Build standalone AOT Jepsen uberjar (jepsen 0.3.10 + jnats 2.24.1 + jepsen.rewind)
#    and pack into jepsen-nats.erofs with OpenJDK 21 headless.
echo "Building Jepsen uberjar & packing ${OUT_DIR}/jepsen-nats.erofs via Docker (${DOCKER_PLATFORM})..."
mkdir -p "${HOME}/.m2"
docker run --rm --platform "${DOCKER_PLATFORM}" \
  -v "${SCRIPT_DIR}:/src:ro" \
  -v "${OUT_DIR}:/out" \
  -v "${HOME}/.m2:/root/.m2" \
  clojure:temurin-21-tools-deps-bookworm bash -c '
set -euo pipefail
apt-get update -qq
apt-get install -y -qq --no-install-recommends erofs-utils >/dev/null
cp -a /src /work
cd /work
clj -T:build uber
ls -lh /work/target/jepsen-nats.jar

mkdir -p /rootfs
cp -a /bin /etc /lib /sbin /usr /opt /rootfs/
mkdir -p /rootfs/dev /rootfs/proc /rootfs/sys /rootfs/tmp /rootfs/build /rootfs/root /rootfs/run /rootfs/opt/jepsen
cp /work/target/jepsen-nats.jar /rootfs/opt/jepsen/jepsen-nats.jar
printf "%s\n%s\n" /opt/java/openjdk/lib /opt/java/openjdk/lib/server > /rootfs/etc/ld.so.conf.d/openjdk.conf
chroot /rootfs ldconfig
chroot /rootfs /opt/java/openjdk/bin/java -version
mkfs.erofs -zlz4 /out/jepsen-nats.erofs /rootfs
ls -lh /out/jepsen-nats.erofs
'

echo "Built:"
ls -lh "${OUT_DIR}/nats-${NATS_VERSION}.erofs" "${OUT_DIR}/jepsen-nats.erofs"
