#!/bin/sh
set -eu

until (nc -z 10.0.0.1 5432 2>/dev/null || bash -c 'exec 3<>/dev/tcp/10.0.0.1/5432' 2>/dev/null); do
  sleep 0.25
done
echo DB_PORT_OPEN
exec umami-server
