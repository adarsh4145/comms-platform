#!/usr/bin/env bash
# Provisions the topics before any application connects.
#
# The Spring Cloud Stream binder can create these itself, but then whichever service starts first
# decides the layout. Settling it here makes the topology explicit and keeps it in one place
# instead of spread across four binder configs.
#
# Partitions are weighted by priority, because partitions are the unit of consumer parallelism:
# a topic can never be worked on by more threads than it has partitions. The weights line up with
# the consumer concurrency configured in config-repo-*/dispatchService.yaml, so at unit=1 each
# consumer thread owns exactly one partition.
#
#   notification.critical         4 x unit   (consumer concurrency 4)
#   notification.high             3 x unit   (concurrency 3)
#   notification.medium           2 x unit   (concurrency 2)
#   notification.low              1 x unit   (concurrency 1)
#   notification.delivery.status  2 x unit   (ingestionService concurrency 2)
#
# KAFKA_PARTITION_UNIT scales the whole set: 1 locally, 2 gives 8/6/4/2, and so on. Raise the
# consumer concurrency with it or the extra partitions just sit idle.
#
# Idempotent: --if-not-exists leaves existing topics untouched. A topic that already exists with a
# different partition count is reported and left alone, because raising a partition count re-maps
# keys to partitions and breaks per-recipient ordering. Set KAFKA_RECREATE_TOPICS=true to drop and
# rebuild the mismatched ones - destructive, and only sensible in a development environment.
set -euo pipefail

BROKER="${KAFKA_BROKER:-comms-kafka:29092}"
UNIT="${KAFKA_PARTITION_UNIT:-1}"
RECREATE="${KAFKA_RECREATE_TOPICS:-false}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"

# topic:weight
TOPICS="
notification.critical:4
notification.high:3
notification.medium:2
notification.low:1
notification.delivery.status:2
"

topics_cmd() { /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" "$@"; }

# Prints the partition count, or nothing at all when the topic does not exist yet.
# kafka-topics.sh exits non-zero for an unknown topic, which under `set -e` aborts the whole
# script, so the failure is absorbed here rather than left to propagate.
partition_count() {
  local described
  described="$(topics_cmd --describe --topic "$1" 2>/dev/null | head -1 || true)"
  printf '%s' "$described" | sed -e "s/.*PartitionCount: //" -e "s/[^0-9].*//"
}

echo "waiting for kafka at $BROKER..."
until topics_cmd --list >/dev/null 2>&1; do
  sleep 3
done

echo "partition unit = $UNIT"

for entry in $TOPICS; do
  topic="${entry%%:*}"
  weight="${entry##*:}"
  want=$((weight * UNIT))

  have="$(partition_count "$topic")"

  if [ -n "$have" ] && [ "$have" != "$want" ] && [ "$RECREATE" = "true" ]; then
    echo "recreating $topic: $have -> $want partitions"
    topics_cmd --delete --topic "$topic"
    # Deletion is asynchronous; wait for the name to disappear before recreating it.
    until [ -z "$(partition_count "$topic")" ]; do sleep 1; done
    have=""
  fi

  topics_cmd --create --if-not-exists --topic "$topic" \
    --partitions "$want" --replication-factor "$REPLICATION" >/dev/null

  have="$(partition_count "$topic")"

  if [ "$have" = "$want" ]; then
    printf 'ok    %-30s %s partitions (%sx%s)\n' "$topic" "$have" "$weight" "$UNIT"
  else
    printf 'WARN  %-30s has %s partitions, wanted %s\n' "$topic" "$have" "$want"
    echo "      Left alone: raising a partition count re-maps keys to partitions and breaks"
    echo "      ordering. Re-run with KAFKA_RECREATE_TOPICS=true to drop and rebuild it."
  fi
done
