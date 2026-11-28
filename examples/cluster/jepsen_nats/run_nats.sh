#!/bin/sh
set -eu
NAME="$1"
shift

STORE_DIR="${NATS_STORE_DIR:-/data/nats-data}"
SYNC_INTERVAL="${NATS_SYNC_INTERVAL:-2m}"
mkdir -p "$STORE_DIR"

ROUTES=""
for peer in n1 n2 n3; do
  if [ "$peer" != "$NAME" ]; then
    ROUTES="${ROUTES}    nats://${peer}:6222
"
  fi
done

cat > /tmp/nats.conf <<EOF
server_name: "${NAME}"
port: 4222

jetstream {
  store_dir: "${STORE_DIR}"
  max_memory_store: 134217728
  max_file_store: 536870912
  sync_interval: "${SYNC_INTERVAL}"
}

cluster {
  name: jepsen-cluster
  listen: 0.0.0.0:6222
  routes: [
${ROUTES}  ]
  ping_interval: 2s
}

accounts {
  \$SYS {
    users: [{user: sys, password: jepsenpw}]
  }
  jepsen {
    jetstream: enabled
    users: [{user: jepsen, password: jepsenpw}]
  }
}
EOF

exec /usr/local/bin/nats-server -c /tmp/nats.conf "$@"
