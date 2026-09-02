#!/usr/bin/env bash
# Elects the replica set, once.
#
# mongod starts with --replSet but a replica set does not elect itself: without rs.initiate there
# is no primary, and ingestionService's transactional outbox fails on its first write. This used
# to be a manual mongosh step written down in note.txt, which meant "clone the repo and run it"
# was not actually true.
#
# Idempotent: rs.status() only throws NotYetInitialized on a virgin data directory, so every later
# start exits immediately without touching anything.
set -euo pipefail

URI="mongodb://admin:admin@comms-mongo:27017/?authSource=admin&directConnection=true"

echo "waiting for mongod..."
until mongosh --quiet "$URI" --eval "db.adminCommand({ping:1})" >/dev/null 2>&1; do
  sleep 2
done

mongosh --quiet "$URI" --eval '
  try {
    rs.status();
    print("replica set rs0 already initialised - nothing to do");
  } catch (e) {
    print("initialising replica set rs0");
    rs.initiate({_id: "rs0", members: [{_id: 0, host: "comms-mongo:27017"}]});
  }
'
