# comms-platform — service guide

## 1. Service responsibilities, in startup order

Services must start in this order because each one depends on something the previous one provides.

### 1. eurekaServer
**What it does:** Service discovery registry. Every other service registers itself here on startup, and looks up other services by name instead of a hardcoded host/port.

**Why it exists:** Without it, `cloudGateway` would need a hardcoded list of every backend's address, and that list breaks the moment a service moves, scales, or restarts on a different port.

**How it works:** Runs standalone (`register-with-eureka: false`, `fetch-registry: false`) — it's the one service designed to have zero external dependencies, so it can always come up first. Every other service's `eureka.client.service-url.defaultZone` points at it.

**Port:** `8025`

---

### 2. configServer
**What it does:** Centralized configuration. Every service (except itself and eurekaServer) fetches its own properties from here at startup instead of bundling them in its own jar.

**Why it exists:** Lets you change a property (a topic name, a failure rate, a connection string) in one place and have it apply without rebuilding the service.

**How it works:** Backed by two profiles — `native` (reads from local disk, `config-repo-native/`, used when running via IntelliJ) and `github` (reads from the pushed GitHub repo, `config-repo-github/`, used once services are containerized). Registers with Eureka using a locally-hardcoded Eureka URL, since it can't fetch that from itself.

**Port:** `8020`

---

### 3. cloudGateway
**What it does:** The single external entry point. Routes incoming requests to the correct backend service by name, using Eureka's discovery locator — no hand-written routes needed.

**Why it exists:** Callers (Bruno, upstreamSimulator, eventually real clients) shouldn't need to know which port each internal service runs on. They call the Gateway; it figures out where to send the request.

**How it works:** Reactive (WebFlux/Netty-based). `spring.cloud.gateway.server.webflux.discovery.locator.enabled: true` auto-generates a route per registered service (lowercased). A `Retry` filter with `CacheRequestBody` absorbs brief unavailability (e.g. a service still registering with Eureka) by retrying up to 5 times with backoff before giving up.

**Port:** `8030`

---

### 4. upstreamSimulator
**What it does:** Simulates an external client system (e.g. an order service) that triggers notifications. Currently a bare REST skeleton — accepts a request and logs it.

**Why it exists:** Represents the "upstream" caller in the architecture, standing in for whatever real system would eventually be requesting notifications be sent.

**Status:** Not yet wired to actually call ingestionService — see the pending items list. Right now it only proves it can run inside the ecosystem (Eureka + Config Server registered).

**Port:** `8035`

---

### 5. ingestionService
**What it does:** The entry point for notification requests. Validates the request, checks for duplicates (idempotency), persists it, and reliably hands it off to Kafka for downstream processing.

**Why it exists:** This is where "did we already receive this exact request" gets decided, and where the request becomes durable — nothing gets lost even if Kafka is briefly unavailable.

**How it works:**
- `POST /notifications` → checks Redis for the `Idempotency-Key` header first.
- If new: saves a `NotificationRequest` **and** an `OutboxEvent` in a single MongoDB transaction (the transactional outbox pattern) — so the two writes either both happen or neither does.
- A scheduled relay (`OutboxRelay`, every 5s) reads `PENDING` outbox events and publishes them to the correct priority-based Kafka topic (`notification.critical/high/medium/low`) via Spring Cloud Stream's `StreamBridge`.

**Port:** `8040`

---

### 6. dispatchService
**What it does:** Consumes notification events from Kafka and hands them off to providerService for actual delivery.

**Why it exists:** Decouples "a notification needs to be sent" (ingestionService's concern) from "actually sending it" (providerService's concern) — Kafka is the buffer between them, so a slow or failing provider doesn't block ingestion.

**How it works:** Four separate `Consumer<String>` functions, one per priority topic, each with its own concurrency setting (`critical: 4, high: 3, medium: 2, low: 1`) — higher-priority topics get more parallel consumer threads. Each consumed event is parsed and sent to providerService via a Feign client, resolved through Eureka.

**Port:** `8045`

---

### 7. providerService
**What it does:** Mock downstream provider. Simulates real-world email/SMS delivery — randomized latency, randomized failure rate — and performs genuine delivery (SMTP for email, HTTP for SMS) to Buggregator.

**Why it exists:** Gives the rest of the system something realistically unreliable to build resilience patterns against, without needing a real third-party provider account.

**How it works:** `POST /provider/send` — rolls a random delay and failure chance (config-driven, currently 20%) before attempting delivery. On success, sends real SMTP mail or a real HTTP POST mimicking Twilio's API format, both landing in Buggregator.

**Port:** `8050`

---

## 2. Running the whole project

### Step 1 — Bring up infrastructure
From the repo root:
```bash
docker compose up -d
```
This starts MongoDB (replica set), PostgreSQL, Redis, Kafka (KRaft), DBGate, kafka-ui, and Buggregator.

**Verify:**
- DBGate: `http://localhost:8094` — should show connections to Postgres, Mongo, Redis
- kafka-ui: `http://localhost:8090` — cluster should show as online
- Buggregator: `http://localhost:8000` — UI should load

### Step 2 — Start services, in order
```
eurekaServer   → wait for it to be reachable at :8025
configServer   → wait for it to register in Eureka
cloudGateway   → wait for it to register in Eureka
ingestionService
dispatchService
providerService
upstreamSimulator
```
Each can be started from IntelliJ's run configurations. Confirm each one appears as `UP` in the Eureka dashboard (`http://localhost:8025`) before moving to the next — while not strictly required for every service, it removes a variable when debugging.

### Step 3 — Confirm the pipeline works
```bash
curl -X POST http://localhost:8030/ingestionservice/notifications \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: manual-test-001" \
  -d '{"recipient": "test@example.com", "message": "Setup check", "priority": "HIGH"}'
```
Then check:
- DBGate → `notification_requests` and `outbox_events` collections — should show your new documents
- kafka-ui → `notification.high` topic — should show one message
- dispatchService's console — should log it was dispatched
- Buggregator UI (`http://localhost:8000`) — the email should have actually arrived

If all four show up, the whole system is working end to end.

---

## 3. API examples

### Individual service APIs (called directly, bypassing the Gateway — useful for isolated testing)

**ingestionService — create a notification**
```bash
POST http://localhost:8040/notifications
Content-Type: application/json
Idempotency-Key: order-4471-notify

{
  "recipient": "customer@example.com",
  "message": "Your order has shipped",
  "priority": "HIGH"
}
```
Response (`201 Created`):
```json
{
  "id": "68a1f2c9e4b0a1234567890a",
  "recipient": "customer@example.com",
  "message": "Your order has shipped",
  "priority": "HIGH",
  "status": "RECEIVED",
  "createdAt": "2026-08-30T04:12:03.441Z"
}
```

**providerService — send directly (bypasses Kafka entirely — for testing the provider in isolation)**
```bash
POST http://localhost:8050/provider/send
Content-Type: application/json

{
  "channel": "EMAIL",
  "sender": "noreply@comms-platform.local",
  "recipient": "customer@example.com",
  "subject": "Order shipped",
  "message": "Your order has shipped"
}
```
Response:
```json
{
  "success": true,
  "providerMessageId": "a3f9c1e2-...",
  "errorMessage": null
}
```

---

### System-wide flow: upstreamSimulator → cloudGateway → … → Buggregator

This is the intended full round trip once upstreamSimulator is wired to actually call ingestionService (currently upstreamSimulator only logs locally — this shows the target flow using the real, already-working pieces downstream of it).

**Hop 1 — Caller triggers upstreamSimulator**
```bash
POST http://localhost:8030/upstreamsimulator/simulate/notification
Content-Type: application/json

{
  "recipient": "customer@example.com",
  "message": "Your order has shipped",
  "priority": "HIGH"
}
```

**Hop 2 — upstreamSimulator forwards to ingestionService, through the Gateway**
*(the piece still to be built — upstreamSimulator would make this call itself)*
```bash
POST http://localhost:8030/ingestionservice/notifications
Idempotency-Key: <generated per upstream event>
Content-Type: application/json

{
  "recipient": "customer@example.com",
  "message": "Your order has shipped",
  "priority": "HIGH"
}
```
`cloudGateway` resolves `ingestionservice` via Eureka discovery locator and proxies the request.

**Hop 3 — ingestionService processes and persists**
- Redis: idempotency key checked, not found
- MongoDB: `NotificationRequest` + `OutboxEvent` saved in one transaction
- Response `201 Created` returned back through the Gateway to the caller
- *(async, within 5s)* `OutboxRelay` publishes to Kafka topic `notification.high`

**Hop 4 — dispatchService consumes**
```
Kafka topic: notification.high
Payload: {"requestId": "...", "recipient": "customer@example.com", "message": "Your order has shipped", "priority": "HIGH"}
```
Consumed by `dispatchHigh` (concurrency: 3), parsed into `NotificationCreatedEvent`.

**Hop 5 — dispatchService calls providerService via Feign**
```
POST http://providerService/provider/send   (resolved via Eureka, not hardcoded)

{
  "channel": "EMAIL",
  "sender": null,
  "recipient": "customer@example.com",
  "subject": "Notification",
  "message": "Your order has shipped"
}
```

**Hop 6 — providerService simulates and delivers**
- Rolls random delay (200–2000ms) and failure chance (20%)
- On success: sends real SMTP mail to Buggregator (`localhost:1025`)
- Returns `{"success": true, "providerMessageId": "...", "errorMessage": null}` back to dispatchService

**Hop 7 — Buggregator receives the real delivery**
Visible at `http://localhost:8000` under the SMTP view — subject "Notification", body "Your order has shipped", recipient `customer@example.com`.

**End-to-end result:** one HTTP call at the very top (Hop 1) results in a real, inspectable email landing in Buggregator, having passed through discovery-based routing, transactional persistence, priority-based Kafka routing, and a resilient service-to-service call — all without any component hardcoding another's address.

+++++++++++++++++++++++++++

HOW CHANNEL IS DECIDED EMAIL OR SMS ??

Right now, it doesn't really "decide" anything — it's hardcoded. Look at NotificationConsumers.handle() in dispatchService:

java
SendRequest sendRequest = new SendRequest(
SendRequest.Channel.EMAIL,   // ← always this, no matter what
null,
event.recipient(),
"Notification",
event.message()
);

Every single notification gets sent as EMAIL, regardless of anything in the original request. providerService itself has zero decision-making role here either — it just executes whatever channel value it's handed, via a plain switch in ProviderSimulationService.send():

java
return switch (request.channel()) {
case EMAIL -> sendEmail(request);
case SMS -> sendSms(request);
};

So the real answer is: the decision point is dispatchService, and currently there is no decision — it's a fixed value. This is a genuine gap worth designing deliberately rather than leaving as-is, since your original project scope explicitly wanted both channels working end to end.

A few reasonable ways to actually decide this, worth thinking through:

Explicit channel field on the original request — the caller (upstreamSimulator, or whoever creates the notification) states up front whether they want email or SMS. Most predictable, but requires NotificationRequest/NotificationCreatedEvent to carry a new field all the way through the pipeline (ingestionService → outbox payload → Kafka → dispatchService).
Infer from the recipient format — an email-shaped string (contains "@") → EMAIL, a phone-number-shaped string → SMS. Convenient (no schema change needed), but fragile and implicit — dispatchService would be guessing intent from data shape rather than being told.
Send to both channels — treat EMAIL and SMS as independent delivery attempts per notification, not a single choice. Matches "send critical alerts via both channels" as a realistic real-world pattern, but means DeliveryAttempt (once you build it) needs to track outcomes per-channel, not one outcome per notification.

Given you already have Priority as a first-class field carried through the whole pipeline, option 1 is the most consistent with how you've built everything else — add a channel field alongside priority in CreateNotificationRequest, NotificationRequest, and NotificationCreatedEvent, and have dispatchService read it instead of hardcoding EMAIL.

+++++++++++++++++++++

https://claude.ai/chat/27fcdc18-d64a-4b62-a083-6addc649bc8e



---

## 4. Distributed tracing

One request from `upstreamSimulator` produces **one trace** spanning all five services, viewable in Grafana.

### Stack

`infra/docker-compose.yml` runs `grafana/otel-lgtm` (`comms-lgtm`) — Grafana + Tempo (traces) + Loki (logs) + Prometheus (metrics) in one container.

| Endpoint | URL |
|---|---|
| Grafana UI | http://localhost:3000 (anonymous admin) |
| OTLP HTTP (what the services push to) | http://localhost:4318 |
| OTLP gRPC | http://localhost:4317 |

Traces are in **Explore → Tempo**. Search by `Service Name = upstreamSimulator`, or paste a trace id straight from a log line.

### How it is wired

- Each service in the request path depends on `spring-boot-starter-opentelemetry` — Micrometer Tracing's OTel bridge plus the OTLP exporter.
- Shared config lives in `config-repo-native/application.yaml` (and its `config-repo-github` twin), so it applies to every config-client service at once: W3C propagation, 100% sampling, the OTLP endpoint, `spring.reactor.context-propagation: auto`, and the `logging.pattern.correlation` that puts `[service,traceId,spanId]` in every log line.
- Kafka spans come from `spring.cloud.stream.kafka.binder.enable-observation: true`, which instruments both the producer and the consumer and injects `traceparent` into record headers.
- Feign needs `io.github.openfeign:feign-micrometer` on the classpath — without it Spring Cloud OpenFeign silently skips its observation capability and the trace stops at dispatchService.
- Outgoing `WebClient` calls must be built from Boot's auto-configured `WebClient.Builder` (from `spring-boot-starter-webclient`). A hand-rolled `WebClient.builder()` has no observation customizer and breaks the trace with no error.

### The outbox gap, and how it is bridged

The transactional outbox is asynchronous: the span that writes the row has ended long before `OutboxRelay` picks it up 5 seconds later on a scheduler thread. Left alone, the Kafka publish would start a brand-new trace.

So `NotificationRequestService` captures the live propagation headers into `OutboxEvent.traceContext` at write time, and `OutboxRelay` re-opens them (`TracePropagation.continueTrace`, in `core`) around the publish. The producer span — and everything downstream of it — then hangs off the original HTTP request.

### What a full trace looks like

```
upstreamSimulator  http post /simulate/notification
  └─ http post                             (WebClient → gateway)
cloudGateway       http post → HTTP POST   (routed to lb://INGESTIONSERVICE)
ingestionService   http post /notifications
  ├─ get / set                             (Redis idempotency)
  ├─ notification_requests.insert          (Mongo)
  ├─ outbox_events.update                  (Mongo)
  └─ outbox.relay.publish                  (re-opened trace, +5s)
       └─ streamBridge process → notification.high send
dispatchService    notification.high process → dispatchHigh process
  └─ HTTP POST                             (Feign → providerService)
providerService    http post /provider/send
  └─ provider.send                         (SMTP → Buggregator)
```

`JavaMailSender` has no built-in instrumentation, so `provider.send` is an explicit `Observation` in `ProviderSimulationService`. Mongo spans exist only because `MongoConfig` installs `MongoObservationCommandListener` by hand — the client is built manually there, so Boot's customizers never run.

### Sampling

`management.tracing.sampling.probability: 1.0` is a local-development setting. Lower it before running anything resembling load.

---

## 5. Delivery lifecycle and the dead-letter queue

### Notification lifecycle

A `NotificationRequest` used to be written as `RECEIVED` and stay there forever — nothing ever told ingestionService what happened downstream. It now moves through:

| Status | Set by | When |
|---|---|---|
| `RECEIVED` | ingestionService | Request persisted with its outbox row, in one transaction |
| `PROCESSING` | ingestionService (`OutboxRelay`) | Event successfully published to Kafka |
| `SENT` / `FAILED` | ingestionService (`DeliveryStatusConsumer`) | dispatchService reported the terminal outcome |

dispatchService publishes a `NotificationDeliveryEvent` to **`notification.delivery.status`** after every terminal outcome; ingestionService consumes it (`notificationDeliveryStatus`) and applies an atomic `updateFirst`, also recording `providerMessageId`, `deliveryAttempts` and `deliveryError`. Because the publish happens inside the consumer observation, the status update stays in the **same trace** as the delivery it describes.

### Dead-letter queue (Postgres)

`dispatch.delivery.max-attempts` (default 3, 2s backoff) retries in place on the consumer thread — deliberately, so per-partition ordering holds. `success=false` from providerService now counts as a failure; it used to be logged and forgotten.

When the attempts are exhausted the event lands in the `dead_letter_event` table with the verbatim payload, the **trace id**, the failure reason and the stack trace. An unparseable payload skips the retries entirely (`UNPARSEABLE_PAYLOAD`) — a poison pill does not get better on the fourth attempt.

| Endpoint (dispatchService, `:8045`) | Purpose |
|---|---|
| `GET /dlq?status=NEW&limit=50` | Recent dead letters |
| `GET /dlq/by-trace/{traceId}` | Everything dead-lettered in one trace |
| `POST /dlq/{id}/replay` | Re-runs the original delivery through the consumer's own path |
| `POST /dlq/{id}/discard` | Marks a row as deliberately abandoned |

Schema is created by `ddl-auto: update` against the `comms_platform` Postgres database — the one that was running unused.

### Outbox relay concurrency

The relay claims rows with an atomic `findAndModify` (`PENDING → PROCESSING`), so two ingestion instances can't publish the same event. A crashed instance's claims are returned to `PENDING` after two minutes. Each scheduled pass is guarded by an `AtomicBoolean`, because `@Scheduled(fixedDelay)` measures from method *return* and a reactive `subscribe()` returns immediately.

---

## 6. Building images

Jars are built by the reactor; the image is runtime-only.

```bash
mvn -q package -DskipTests
docker build --build-arg MODULE=ingestionService -t comms-platform/ingestion-service .
```

Images carry no environment config. Point them at their dependencies at run time with `CONFIG_SERVER_URL`, `EUREKA_SERVER_URL`, and (for upstreamSimulator) `GATEWAY_URL`; everything else comes from the config server, where `config-repo-github` now addresses the compose DNS names with per-value env overrides.

> `spotless:apply` is bound to `process-sources`, so every build normalises formatting. That is why a build touches files you did not edit.


https://claude.ai/code/artifact/3d8157a9-54bd-4ac8-9ed8-ce352971f7f7
