# comms-platform

A notification platform built as seven Spring Boot services: an HTTP request comes in, is stored
with a transactional outbox, published to Kafka by priority, dispatched to an email/SMS provider,
and the outcome flows back. Traces, logs and metrics for the whole path land in one Grafana stack.

Architecture and port map: [`documents/`](documents/) — start with
[`comms-platform-guide.md`](documents/comms-platform-guide.md). Known gaps and what to pick up
next: [`documents/nextplan.md`](documents/nextplan.md).

---

## 1. Prerequisites

| | |
|---|---|
| **JDK 25** | The build fails fast with a clear message on anything older. |
| **Docker Desktop** | Running, with ~4 GB available to it. |
| **Maven 3.9+** | There is no Maven wrapper in this repo, so Maven has to be on `PATH`. |
| **IntelliJ IDEA** | Optional — the services are plain `java -jar` otherwise. |

## 2. Build

```bash
mvn clean install
```

> Use `clean` after changing anything in `core/`. A change confined to `core` does **not** trigger
> repackaging of the other services' fat jars, so without it they keep embedding the old `core`
> and you will debug behaviour that is no longer in the source.

## 3. Start the infrastructure

```bash
cd infra
docker compose up -d
```

That brings up MongoDB, PostgreSQL, Redis, Kafka, Buggregator (a fake email/SMS sink), the Grafana
LGTM stack, five metrics exporters, and two admin UIs.

**Two containers are meant to exit immediately** — they are one-shot initialisers, and seeing them
in `Exited (0)` is success, not failure:

- `comms-mongo-init` runs `rs.initiate()` so the replica set has a primary. Without it the
  transactional outbox fails on its first write.
- `comms-kafka-init` creates the five topics with partitions weighted by priority.

Both are idempotent — they re-run on every `up` and do nothing when the work is already done.

Check they succeeded:

```bash
docker logs comms-mongo-init
docker logs comms-kafka-init
```

Expect `replica set rs0 already initialised - nothing to do` (or `initialising replica set rs0` the
first time), and five `ok` lines from Kafka.

### Starting it again later, or after a reboot

`docker compose up -d` is always the right command. It is safe to run any number of times.

**Both init containers run again on every `up`** — they are not skipped, and they are not supposed
to be. The idempotency lives inside the scripts: `mongo-init` only calls `rs.initiate()` when
`rs.status()` says the set was never initialised, and `kafka-init` only creates topics that do not
exist. On a second run they look at the world, find it already correct, print what they found and
exit 0.

This was verified by killing every container outright — the closest thing to pulling the power —
and bringing the stack back with `docker compose up -d`:

- `mongo-init` re-ran and reported *already initialised — nothing to do*
- MongoDB re-elected itself: `rs0 -> PRIMARY`
- all five Kafka topics were still there, and `kafka-init` changed nothing
- both init containers exited 0

**One quirk worth knowing after a laptop restart.** Only the five metrics exporters carry
`restart: unless-stopped`, so Docker Desktop brings those back on boot while the databases they
read from stay stopped. They will restart-loop harmlessly until you run `docker compose up -d`.
Nothing is broken; the noise stops as soon as the stack is up.

**If `kafka-init` prints `WARN ... has N partitions, wanted M`**, your topics predate a change to
the partition weights. It deliberately will not fix that on its own, because changing a partition
count re-maps keys to partitions and breaks per-recipient ordering. To adopt the new layout in a
development environment:

```bash
KAFKA_RECREATE_TOPICS=true docker compose up -d kafka-init
```

That drops and rebuilds the mismatched topics — along with anything still sitting in them.

## 4. Start the Java services

Order matters for the first two:

| # | Service | Port | Actuator | Why here |
|---|---|---|---|---|
| 1 | `eurekaServer` | 8025 | 9025 | Registry — everything else registers with it |
| 2 | `configServer` | 8020 | 9020 | Serves configuration; the next four **fail to start without it** |
| 3 | `cloudGateway` | 8030 | 9030 | Entry point |
| 4 | `ingestionService` | 8040 | 9040 | |
| 5 | `dispatchService` | 8045 | 9045 | |
| 6 | `providerService` | 8050 | 9050 | |
| 7 | `upstreamSimulator` | 8035 | 9035 | Stands in for an external caller |

Services 3–7 can start in any order once 1 and 2 are up.

```bash
java -jar eurekaServer/target/eurekaServer-0.0.1-SNAPSHOT.jar
java -jar configServer/target/configServer-0.0.1-SNAPSHOT.jar
java -jar cloudGateway/target/cloudGateway-0.0.1-SNAPSHOT.jar
java -jar ingestionService/target/ingestionService-0.0.1-SNAPSHOT.jar
java -jar dispatchService/target/dispatchService-0.0.1-SNAPSHOT.jar
java -jar providerService/target/providerService-0.0.1-SNAPSHOT.jar
java -jar upstreamSimulator/target/upstreamSimulator-0.0.1-SNAPSHOT.jar
```

In IntelliJ, run each module's `*Application` class in the same order.

Confirm all seven are up — actuator is on its **own** port, not the service port:

```bash
for p in 9020 9025 9030 9035 9040 9045 9050; do
  printf '%s -> %s\n' "$p" "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$p/actuator/health)"
done
```

Seven `200`s. You can also open the Eureka dashboard at <http://localhost:8025> — six applications
register (eurekaServer does not register with itself).

## 5. Send a request end to end

```bash
curl -i -X POST http://localhost:8030/ingestionservice/notifications \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(date +%s)" \
  -d '{
        "recipient": "someone@example.com",
        "from": "noreply@comms.local",
        "message": "Your order has shipped",
        "priority": "HIGH",
        "channel": "EMAIL"
      }'
```

`201 Created` with the stored notification. Within a few seconds:

- **<http://localhost:8000>** — Buggregator, the fake provider. The email appears under the SMTP
  view. Use `"channel": "SMS"` and it appears under the SMS view instead.
- **<http://localhost:8090>** — kafka-ui. `notification.high` has one more message.

`Idempotency-Key` is what makes the endpoint safe to retry: send the same key twice and you get
the same notification back rather than a second send.

**On a cold start the first request may 404 or log `No servers available for service:
providerService`.** That is Eureka registration still propagating; the gateway retries and the
dispatcher retries up to three times. Wait ~30s after starting the services and it settles.

## 6. Where to look in Grafana

<http://localhost:3000> — no login, anonymous admin.

### Dashboards

Under **Dashboards → comms-platform**:

| Dashboard | Use it for |
|---|---|
| **Platform Health** | Start here. Is every Java service and every infrastructure component up? |
| **SpringBoot APM Dashboard** | Per-service JVM internals — heap, GC, threads, HTTP rate/latency. Pick the service from the **application** dropdown. |
| **JVM (Micrometer)** | The same ground in more depth. |
| **Kafka Exporter Overview** | Consumer lag per group, message rates, partitions per topic. |
| **PostgreSQL / MongoDB / Redis** | Per-store internals. |
| **cAdvisor Docker Insights** | CPU and memory per container. |

Two things that look broken and are not: `-1` consumer lag means "this group has never committed
on that partition yet", and the JVM dashboards paint blank for a few seconds while ~30 queries
resolve. More of these are listed in
[`infra/grafana/dashboards/README.md`](infra/grafana/dashboards/README.md).

### Logs

**Explore → Loki**. Every service ships its logs here, tagged with `service_name`.

```logql
{service_name="ingestionService"}                  # one service
{service_name=~"ingestionService|dispatchService"} # several
{service_name=~".+"} |= "Dispatched notification"  # full-text across all of them
```

## 7. Following one request across all services

This is the part worth learning. Every log line and every span carries the same `trace_id`, so one
query returns the whole journey.

### The easy way — choose the trace id yourself

The platform propagates W3C `traceparent`, so if the caller supplies one, everything downstream
adopts it. That means you don't have to go hunting for the id afterwards:

```bash
TRACE=$(openssl rand -hex 16)          # any 32 hex characters will do
SPAN=$(openssl rand -hex 8)
echo "trace id: $TRACE"

curl -s -o /dev/null -X POST http://localhost:8030/ingestionservice/notifications \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "traceparent: 00-$TRACE-$SPAN-01" \
  -d '{"recipient":"someone@example.com","from":"noreply@comms.local",
       "message":"traced request","priority":"HIGH","channel":"EMAIL"}'
```

Then in **Explore → Loki**, paste that id into:

```logql
{service_name=~".+"} | trace_id = "<your trace id>"
```

Switch the time range to **Last 15 minutes** and sort ascending. You get the full path in order —
roughly 60 lines across four services:

```
cloudGateway      POST /ingestionservice/notifications -> 201 CREATED in 233 ms
ingestionService  request received: CreateNotificationRequest[...]
ingestionService  saving notification request, NotificationRequest(id=6a98443a...)
ingestionService  saving outbox event, OutboxEvent(id=9351d283-...)
ingestionService  transactional save notification request and event in DB success
ingestionService  saved idempotency in redis: true
ingestionService  Relayed outbox event 9351d283-... for notification 6a98443a...
dispatchService   [HIGH] event 9351d283-... originated in trace 12f2c1e1...
dispatchService   [HIGH] Dispatched notification 6a98443a... on attempt 2 -> providerMessageId=...
providerService   sending mail: SendRequest[channel=EMAIL, ...]
ingestionService  Notification 6a98443a... moved to SENT
```

Note that `trace_id` is **structured metadata**, not part of the log text — which is why the filter
is `| trace_id = "..."` and not `|= "..."`.

### The waterfall

**Explore → Tempo**, query type **TraceQL**, paste the trace id. You get ~25 spans across the four
services showing where the time actually went: the gateway hop, the Mongo writes, the Kafka
publish, the Feign call to providerService, the SMTP send.

### Jumping between the two

You rarely need to copy ids by hand:

- **Loki → Tempo.** Expand any log row. Under **Links** (below the fields list, *not* the
  `trace_id` value itself — that one only offers copy) there is a `Trace: <id>` chip that opens
  the trace.
- **Tempo → Loki.** Click any span, then **Related logs** in its detail panel. That runs the Loki
  query for you, scoped to that service and trace.

### Finding a trace when you didn't choose the id

Search Loki for something you do know — a notification id, a recipient, an error — then use the
**Links** chip on the matching line:

```logql
{service_name=~".+"} |= "6a98443ad7af5bc02a3eb658"
```

## 8. Shutting down

Stop the Java processes, then:

```bash
cd infra
docker compose down          # keeps all data under infra/data/
docker compose down -v       # also drops the mongo keyfile volume
```

Telemetry, databases and Kafka logs live in `infra/data/`. Deleting a subfolder there resets that
component on the next `up` — the init containers will rebuild the replica set and the topics.

## 9. Useful URLs

| | |
|---|---|
| Grafana | <http://localhost:3000> |
| Buggregator (captured email/SMS) | <http://localhost:8000> |
| Eureka dashboard | <http://localhost:8025> |
| kafka-ui | <http://localhost:8090> |
| DBGate (Postgres, Mongo, Redis) | <http://localhost:8094> |
| Gateway | <http://localhost:8030> |
