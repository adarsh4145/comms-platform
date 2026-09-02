#!/usr/bin/env bash
# Starts mongod as a single-node replica set.
#
# The replica set is not optional: ingestionService writes the notification and its outbox row in
# one transaction, and MongoDB only offers multi-document transactions on a replica set. Running
# with --replSet in turn requires internal authentication, hence the keyfile - generated on first
# start and kept in a named volume so it survives a wipe of the data directory.
#
# rs.initiate() is NOT run here; see mongo-init.sh.
set -euo pipefail

KEYFILE=/keyfile/mongo-keyfile

if [ ! -f "$KEYFILE" ]; then
  echo "generating replica set keyfile"
  openssl rand -base64 756 > "$KEYFILE"
  chmod 400 "$KEYFILE"
  chown mongodb:mongodb "$KEYFILE"
fi

exec docker-entrypoint.sh mongod --replSet rs0 --bind_ip_all --keyFile "$KEYFILE"
