package io.github.adarsh4145.dispatchService.service;

import io.github.adarsh4145.core.event.EventSerde;
import io.github.adarsh4145.core.event.NotificationCreatedEvent;
import io.github.adarsh4145.core.event.NotificationDeliveryEvent;
import io.github.adarsh4145.core.provider.SendRequest;
import io.github.adarsh4145.core.provider.SendResponse;
import io.github.adarsh4145.dispatchService.client.ProviderServiceClient;
import io.github.adarsh4145.dispatchService.domain.DeadLetterEvent;
import io.github.adarsh4145.dispatchService.exception.ProviderDeliveryException;
import io.github.adarsh4145.dispatchService.publisher.DeliveryStatusPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Owns "turn one event into one delivery attempt, and do something honest when it fails".
 *
 * <p>Kept out of the consumer so the DLQ replay endpoint can reuse exactly the same path.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationDispatchService {

  private static final String SUBJECT = "Notification";

  private final ProviderServiceClient providerServiceClient;
  private final DeadLetterService deadLetterService;
  private final EventSerde eventSerde;
  private final DeliveryStatusPublisher deliveryStatusPublisher;

  @Value("${dispatch.delivery.max-attempts:3}")
  private int maxAttempts;

  @Value("${dispatch.delivery.retry-backoff-ms:2000}")
  private long retryBackoffMs;

  /**
   * Retries in place on the consumer thread, which holds the partition. That is deliberate: it
   * preserves per-partition ordering, and the backoff budget is small enough to stay well inside
   * max.poll.interval.ms.
   */
  public boolean dispatch(String priorityLabel, String payload) {
    NotificationCreatedEvent event;
    try {
      event = eventSerde.fromJson(payload, NotificationCreatedEvent.class);
    } catch (RuntimeException ex) {
      // A payload we cannot parse is a poison pill; no number of retries will fix it.
      deadLetterService.record(
          priorityLabel, null, payload, DeadLetterEvent.FailureReason.UNPARSEABLE_PAYLOAD, ex, 0);
      return false;
    }

    log.info(
        "[{}] event {} originated in trace {}",
        priorityLabel,
        event.getEventId(),
        event.getTraceId());

    Throwable lastFailure = null;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        SendResponse response = providerServiceClient.send(toSendRequest(event));
        if (response.success()) {
          log.info(
              "[{}] Dispatched notification {} on attempt {} -> providerMessageId={}",
              priorityLabel,
              event.getRequestId(),
              attempt,
              response.providerMessageId());
          deliveryStatusPublisher.publish(
              event,
              NotificationDeliveryEvent.DeliveryStatus.SENT,
              response.providerMessageId(),
              null,
              attempt);
          return true;
        }
        lastFailure = new ProviderDeliveryException(response.errorMessage());
      } catch (Exception ex) {
        lastFailure = ex;
      }

      log.warn(
          "[{}] Delivery attempt {}/{} failed for notification {}: {}",
          priorityLabel,
          attempt,
          maxAttempts,
          event.getRequestId(),
          lastFailure.getMessage());

      if (attempt < maxAttempts) {
        sleepBeforeRetry();
      }
    }

    deadLetterService.record(
        priorityLabel,
        event,
        payload,
        DeadLetterEvent.FailureReason.DELIVERY_FAILED,
        lastFailure,
        maxAttempts);
    deliveryStatusPublisher.publish(
        event,
        NotificationDeliveryEvent.DeliveryStatus.FAILED,
        null,
        lastFailure.getMessage(),
        maxAttempts);
    return false;
  }

  private SendRequest toSendRequest(NotificationCreatedEvent event) {
    return new SendRequest(
        event.getChannel(), event.getFrom(), event.getRecipient(), SUBJECT, event.getMessage());
  }

  private void sleepBeforeRetry() {
    try {
      Thread.sleep(retryBackoffMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
