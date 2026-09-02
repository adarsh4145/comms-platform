package io.github.adarsh4145.ingestionService.service;

import io.github.adarsh4145.core.event.EventSerde;
import io.github.adarsh4145.core.event.NotificationCreatedEvent;
import io.github.adarsh4145.core.ingestion.CreateNotificationRequest;
import io.github.adarsh4145.core.provider.SendRequest;
import io.github.adarsh4145.core.tracing.TracePropagation;
import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import io.github.adarsh4145.ingestionService.domain.OutboxEvent;
import io.github.adarsh4145.ingestionService.repository.NotificationRequestRepository;
import io.github.adarsh4145.ingestionService.repository.OutboxEventRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Service
@Slf4j
public class NotificationRequestService {

  private static final String IDEMPOTENCY_KEY_PREFIX = "idempotency:notification:";
  private static final String EVENT_TYPE = "NotificationCreated";
  private static final String SOURCE_IDENTIFIER = "ingestionService";

  private final NotificationRequestRepository notificationRequestRepository;
  private final OutboxEventRepository outboxEventRepository;
  private final TransactionalOperator transactionalOperator;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final EventSerde eventSerde;
  private final TracePropagation tracePropagation;

  /**
   * How long a duplicate request keeps resolving to the same notification. Configurable because
   * the window that suits a laptop is not the one that suits a deployed environment.
   */
  @Value("${notification.idempotency.ttl:24h}")
  private Duration idempotencyTtl;

  public Mono<NotificationRequest> createNotification(
      String idempotencyKey, CreateNotificationRequest request) {
    String redisKey = IDEMPOTENCY_KEY_PREFIX + idempotencyKey;

    return redisTemplate
        .opsForValue()
        .get(redisKey)
        .flatMap(notificationRequestRepository::findById)
        .doOnNext(
            notificationRequest ->
                log.info("existing notification found: {}", notificationRequest.toString()))
        .switchIfEmpty(
            Mono.defer(
                () -> {
                  log.info("new notification request: {}", request.toString());
                  return processNewNotification(redisKey, request);
                }))
        .doOnError(
            throwable ->
                log.error("error processing notification. error: {}", throwable.getMessage()));
  }

  private Mono<NotificationRequest> processNewNotification(
      String redisKey, CreateNotificationRequest request) {

    NotificationRequest notification =
        NotificationRequest.builder()
            .recipient(request.recipient())
            .message(request.message())
            .priority(NotificationRequest.Priority.valueOf(request.priority().name()))
            .status(NotificationRequest.Status.RECEIVED)
            .createdAt(Instant.now())
            .channel(SendRequest.Channel.valueOf(request.channel().toString()))
            .from(request.from())
            .build();

    Mono<NotificationRequest> flow =
        notificationRequestRepository
            .save(notification)
            .doOnNext(saved -> log.info("saving notification request, {}", saved))
            .doOnError(
                throwable ->
                    log.error(
                        "failed saving notification request in mongodb, error: {}",
                        throwable.getMessage()))
            .flatMap(
                savedNotification -> {
                  // Captured while the HTTP span is still current. By the time the relay runs,
                  // this context survives only because it is stored on the outbox row.
                  Map<String, String> traceCarrier = tracePropagation.captureCurrent();
                  String traceId = tracePropagation.currentTraceId();
                  if (traceCarrier.isEmpty()) {
                    log.warn(
                        "no active trace context while writing outbox event for notification {}"
                            + " - downstream hops will start a new trace",
                        savedNotification.getId());
                  }

                  String outboxEventId = UUID.randomUUID().toString();

                  OutboxEvent outboxEvent =
                      OutboxEvent.builder()
                          .id(outboxEventId)
                          .notificationId(savedNotification.getId())
                          .eventType(EVENT_TYPE)
                          .sourceIdentifier(SOURCE_IDENTIFIER)
                          .traceId(traceId)
                          .traceContext(traceCarrier)
                          .priority(savedNotification.getPriority())
                          .payload(toJson(outboxEventId, traceId, savedNotification))
                          .status(OutboxEvent.Status.PENDING)
                          .createdAt(Instant.now())
                          .channel(savedNotification.getChannel())
                          .build();

                  return outboxEventRepository
                      .save(outboxEvent)
                      .doOnNext(saved -> log.info("saving outbox event, {}", saved.toString()))
                      .doOnError(
                          throwable ->
                              log.error(
                                  "failed saving event outbox in db. error: {}",
                                  throwable.getMessage()))
                      .thenReturn(savedNotification);
                });

    return transactionalOperator
        .transactional(flow)
        .doOnNext(
            notificationRequest ->
                log.info("transactional save notification request and event in DB success"))
        .doOnError(
            throwable ->
                log.error(
                    "failed saving notification and event. error: {}", throwable.getMessage()))
        // Deliberately after the transaction commits. Redis is not part of it, so writing the
        // key inside meant a rolled-back transaction could still leave a key pointing at a
        // notification that does not exist.
        .flatMap(saved -> rememberIdempotencyKey(redisKey, saved));
  }

  private Mono<NotificationRequest> rememberIdempotencyKey(
      String redisKey, NotificationRequest saved) {
    return redisTemplate
        .opsForValue()
        .set(redisKey, saved.getId(), idempotencyTtl)
        .doOnNext(stored -> log.info("saved idempotency in redis: {}", stored))
        .onErrorResume(
            throwable -> {
              // The notification is already durable; failing the caller here would only invite a
              // retry that creates a second one. Log loudly and accept the weaker guarantee.
              log.error(
                  "failed saving idempotency key in redis, duplicate suppression is degraded for"
                      + " notification {}. error: {}",
                  saved.getId(),
                  throwable.getMessage());
              return Mono.just(false);
            })
        .thenReturn(saved);
  }

  /**
   * Serializes the full {@link NotificationCreatedEvent} envelope. This used to be an ad-hoc map
   * with no envelope fields, so traceId arrived at dispatchService as null.
   */
  private String toJson(String eventId, String traceId, NotificationRequest notification) {
    NotificationCreatedEvent event =
        NotificationCreatedEvent.builder()
            .eventId(eventId)
            .eventType(EVENT_TYPE)
            .createdAt(Instant.now())
            .sourceIdentifier(SOURCE_IDENTIFIER)
            .traceId(traceId)
            .requestId(notification.getId())
            .recipient(notification.getRecipient())
            .from(notification.getFrom())
            .message(notification.getMessage())
            .priority(notification.getPriority().name())
            .channel(notification.getChannel())
            .build();
    return eventSerde.toJson(event);
  }
}
