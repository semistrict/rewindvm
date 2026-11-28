#!/bin/sh
set -eu
IP="$1"
ZONE="$2"
NAME="$3"
CLASS="$4"
shift 4

mkdir -p /tmp/fdb-data /tmp/fdb-log
printf '%s\n' 'rewind:test1234@10.0.0.1:4500,10.0.0.2:4500,10.0.0.3:4500' > /tmp/fdb.cluster

exec /usr/local/bin/fdbserver \
  --cluster-file /tmp/fdb.cluster \
  --listen-address "${IP}:4500" \
  --public-address "${IP}:4500" \
  --datadir /tmp/fdb-data \
  --logdir /tmp/fdb-log \
  --class "${CLASS}" \
  --locality-zoneid="${ZONE}" \
  --locality-machineid="${NAME}" \
  --memory 384MiB \
  --cache-memory 64MiB \
  --storage-memory 64MiB \
  --knob_disable_posix_kernel_aio=1 \
  --knob_min_available_space=1048576 \
  --knob_min_available_space_ratio=0.01 \
  "$@"
