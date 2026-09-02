# Next plan

Gap analysis as of **2026-09-02**, written so this can be picked up cold. Everything below was
checked against the code, not assumed — file paths are given so each claim can be re-verified.

---

## Already done — do not re-plan these

| Capability | Where |
|---|---|
| Transactional outbox | `ingestionService` — notification + outbox row in one Mongo transaction, scheduled relay publishes |
| Idempotency | Redis key per `Idempotency-Key`, TTL configurable (`notification.idempotency.ttl`) |
| Dead letter queue | `dispatchService/.../DeadLetterController` — `/dlq`, `/dlq/by-trace/{traceId}`, `/dlq/{id}/replay`, `/dlq/{id}/discard`, stored in Postgres |
| Retries | `NotificationDispatchService` — in-service, on the consumer thread to preserve partition ordering; binder retry deliberately off |
| Priority isolation | Four priority topics, partitions weighted 4/3/2/1 × `KAFKA_PARTITION_UNIT` to match consumer concurrency |
| Delivery-status feedback | `notification.delivery.status` → ingestionService updates `RECEIVED → SENT/FAILED` |
| Distributed tracing | OTLP → Tempo, one trace across all hops, trace context carried on the outbox row |
| Logs | OTLP → Loki from all seven services, trace-correlated |
| Metrics + dashboards | Prometheus scrape of `/actuator/prometheus`, 8 dashboards provisioned from `infra/grafana/dashboards` |
| Retention | 24h across Tempo/Loki/Prometheus, one `TELEMETRY_RETENTION` var |
| Reproducible start | `mongo-init` elects the replica set, `kafka-init` provisions topics — both idempotent |

**Retry gap worth noting:** backoff is fixed at 2s. No exponential, no jitter — a downstream
outage means every consumer retries in lockstep.

---

## Non-functional

### 1. No timeouts anywhere — do this first

There is no Feign connect/read timeout and no WebClient timeout configured anywhere. A hung
`providerService` blocks a `dispatchService` consumer thread **that holds a Kafka partition** —
that partition stops moving entirely, and the retry loop sleeps 2s per attempt on top of it.

This is the failure a circuit breaker is meant to catch, but timeouts are its prerequisite, and
this is a three-line config change. Highest value-to-effort item in the list.

### 2. Security — nothing at all

No `spring-boot-starter-security` in any module. Every endpoint is open, the config server serves
configuration unauthenticated, and Eureka accepts any registration.

Live today, not theoretical: `config-repo-native/cloudGateway.yaml` sets
`management.endpoint.env.show-values: ALWAYS` with `env` exposed, so anyone who reaches the
actuator port can read the Postgres password.

Scope when picked up: JWT on the gateway, service-to-service auth, config server auth +
`{cipher}` encryption, drop `env` from the exposure list.

### 3. Testing — the biggest structural gap

Eight test classes, eight `@Test` methods, **every one of them `contextLoads`**. Zero coverage of
the outbox transaction, idempotency, retry/dead-letter behaviour, or trace propagation. All of the
logic worth trusting is currently verified only by manual runs.

### 4. The rest, roughly in order

- **Rate limiting** — none. Redis is already there, so gateway `RequestRateLimiter` is cheap.
- **Circuit breaker / bulkhead** — no resilience4j. Worth it *after* timeouts, not before.
- **DB migrations** — `ddl-auto: update` on Postgres. Needs Flyway before any real deploy.
- **No Mongo indexes declared** — the relay polls on status + priority + `claimedAt`. Collection
  scans today; degrades as the outbox grows.
- **Graceful shutdown not configured** — in-flight dispatches are killed on restart, not drained.
- **No alerting** — dashboards exist, but nothing pages anyone. Consumer lag and DLQ growth are
  the two obvious first rules.
- **PII in logs** — `NotificationRequestController` logs the whole request, so recipient emails
  and phone numbers go to Loki and sit there for 24h.
- **No CI**, no Kubernetes manifests or probes (the shared `Dockerfile` exists), no backup or
  restore story, no load test — throughput is currently unknown.

---

## Functional

- **No way to read a notification back.** `POST /notifications` exists; there is no
  `GET /notifications/{id}`. Status moves to `SENT`/`FAILED` in Mongo and no API exposes it — a
  caller can create but never check.
- **No consumer-side dedupe.** Kafka is at-least-once and there is no processed-event table, so a
  rebalance or redelivery sends the same message twice. Ingestion is idempotent; delivery is not.
- **Single provider per channel** — no failover, no provider selection, no per-provider health.
- **No templating** — the message is a raw string. No personalisation, no localisation.
- **No scheduling** — everything sends immediately. No deferred send, no quiet hours.
- **No preferences or opt-out** — nothing models consent, which for SMS and email is usually a
  legal requirement rather than a feature.
- **No bulk send, no attachments, no multi-tenancy** or per-client API keys.

---

## Suggested sequence

1. **Timeouts** (Feign + WebClient) — hours, removes a real hang
2. **`GET /notifications/{id}`** and **consumer-side dedupe** — hours, removes a correctness bug
3. **Auth on everything**, and turn off `show-values: ALWAYS`
4. **Real tests** — makes everything after this safe to change
5. Rate limiting → circuit breaker → Flyway + Mongo indexes → alerting

The first two are small and fix genuine defects. Security is what would embarrass first if this
were ever exposed. Tests are what make the rest safe.

---

## Operational loose ends

- **Nothing is committed.** The whole tracing / observability / port-scheme effort is uncommitted
  in the working tree.
- **Empty-volume start is unverified.** `kafka-init` was proven against a virgin broker in an
  isolated stack, but a full wipe of `infra/data/` and a clean run has never been done — so the
  clone-and-run story is verified in parts, not end to end.
- **Config duplication in providerService.** Its jar-bundled `application.yaml` duplicates
  `provider.sms.gateway-url` and holds the only copy of `default-sender` for both channels. The
  config-server value wins for the duplicated key, so editing the jar copy silently does nothing.
- **`ingestionService`'s local `application.yaml`** narrows actuator exposure to
  `health,info,env`. Harmless because config-server wins, but it reads as though it should break.
- **Two panels still blank** on *JVM Overview (OpenTelemetry)* — Error % and Duration (95%) want
  OTLP-style histogram names. Same class of problem as Classes/Threads, fixable with two more
  recording rules in `infra/prometheus-rules.yaml`.

check forjego, gitea, opendev setup that.
then sonar, snyk, cucumber feature test, test container and unit / integration tests also.
