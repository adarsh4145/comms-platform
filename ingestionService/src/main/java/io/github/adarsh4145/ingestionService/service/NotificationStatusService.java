package io.github.adarsh4145.ingestionService.service;

import io.github.adarsh4145.core.event.NotificationDeliveryEvent;
import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import java.time.Duration;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Single place that moves a NotificationRequest through its lifecycle.
 *
 * <p>Updates go through {@code ReactiveMongoTemplate} rather than a read-modify-write on the
 * repository, so a status change is one atomic document update and concurrent updates cannot
 * clobber each other.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationStatusService {

  private static final Duration BLOCKING_TIMEOUT = Duration.ofSeconds(10);

  private final ReactiveMongoTemplate reactiveMongoTemplate;

  /** Called from the outbox relay once the event is safely on Kafka. */
  public Mono<Void> markProcessing(String notificationId) {
    return update(notificationId, NotificationRequest.Status.PROCESSING, new Update()).then();
  }

  /**
   * Applies a terminal outcome. Runs on the Kafka listener thread, which is a plain pooled thread
   * and not an event loop, so blocking here is safe - and it is what keeps the offset from being
   * committed before the document is actually updated.
   */
  public void applyDeliveryOutcome(NotificationDeliveryEvent event) {
    NotificationRequest.Status status =
        event.getStatus() == NotificationDeliveryEvent.DeliveryStatus.SENT
            ? NotificationRequest.Status.SENT
            : NotificationRequest.Status.FAILED;

    Update update =
        new Update()
            .set("deliveryAttempts", event.getAttempts())
            .set("providerMessageId", event.getProviderMessageId())
            .set("deliveryError", event.getErrorMessage());

    Long modified = update(event.getRequestId(), status, update).block(BLOCKING_TIMEOUT);

    if (modified == null || modified == 0) {
      log.warn(
          "Delivery status {} for notification {} matched no document",
          status,
          event.getRequestId());
    } else {
      log.info("Notification {} moved to {}", event.getRequestId(), status);
    }
  }

  private Mono<Long> update(
      String notificationId, NotificationRequest.Status status, Update update) {
    if (notificationId == null) {
      return Mono.just(0L);
    }
    update.set("status", status).set("statusUpdatedAt", Instant.now());
    return reactiveMongoTemplate
        .updateFirst(
            Query.query(Criteria.where("_id").is(notificationId)),
            update,
            NotificationRequest.class)
        .map(result -> result.getModifiedCount());
  }
}
