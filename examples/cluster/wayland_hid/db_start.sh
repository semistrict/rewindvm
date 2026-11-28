#!/bin/sh
set -eu

for d in /nix/store/*-postgresql-*/bin; do
  if [ -x "$d/initdb" ]; then
    PATH="$d:$PATH"
    break
  fi
done

mkdir -p /tmp/pgdata /tmp/pgrun
initdb -D /tmp/pgdata -U postgres --auth=trust >/dev/null
cat >> /tmp/pgdata/postgresql.conf <<'EOF'
listen_addresses = '0.0.0.0'
port = 5432
unix_socket_directories = '/tmp/pgrun'
fsync = off
synchronous_commit = off
EOF
printf "host all all 0.0.0.0/0 trust\n" >> /tmp/pgdata/pg_hba.conf

pg_ctl -D /tmp/pgdata -l /tmp/pg.log -o "-k /tmp/pgrun" start
psql -h /tmp/pgrun -U postgres -c "CREATE USER umami WITH PASSWORD 'umami' SUPERUSER;"
psql -h /tmp/pgrun -U postgres -c "CREATE DATABASE umami OWNER umami;"
echo PG_READY
pg_ctl -D /tmp/pgdata stop >/dev/null
exec postgres -D /tmp/pgdata -k /tmp/pgrun
