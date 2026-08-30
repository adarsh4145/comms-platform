package io.github.adarsh4145.ingestionService.service;

import io.github.adarsh4145.ingestionService.domain.OutboxEvent;
import io.github.adarsh4145.ingestionService.kafka.OutboxEventValidator;
import io.github.adarsh4145.ingestionService.kafka.publisher.NotificationEventPublisher;
import io.github.adarsh4145.ingestionService.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

  private final OutboxEventRepository outboxEventRepository;
  private final NotificationEventPublisher notificationEventPublisher;
  private final OutboxEventValidator outboxEventValidator;

  @Scheduled(fixedDelay = 5000)
  public void relayPendingEvents() {
    outboxEventRepository
        .findByStatus(OutboxEvent.Status.PENDING)
        .flatMap(this::processEvent)
        .doOnError(error -> log.error("Unexpected error in outbox relay", error))
        .subscribe();
  }

  private Mono<OutboxEvent> processEvent(OutboxEvent event) {
    if (!outboxEventValidator.isValid(event)) {
      log.warn(
          "Outbox event {} is malformed, marking as MALFORMED (will not be retried)",
          event.getId());
      event.setStatus(OutboxEvent.Status.MALFORMED);
      return outboxEventRepository.save(event);
    }

    try {
      boolean sent =
          notificationEventPublisher.publish(
              event.getPriority(), event.getEventType(), event.getPayload());
      event.setStatus(sent ? OutboxEvent.Status.PUBLISHED : OutboxEvent.Status.FAILED);
    } catch (Exception ex) {
      log.error("Transient failure relaying outbox event {}, will retry", event.getId(), ex);
      event.setStatus(OutboxEvent.Status.FAILED);
    }
    return outboxEventRepository.save(event);
  }

  @Scheduled(fixedDelay = 30000)
  public void retryFailedEvents() {
    outboxEventRepository
        .findByStatus(OutboxEvent.Status.FAILED)
        .flatMap(this::processEvent)
        .doOnError(error -> log.error("Unexpected error retrying failed outbox events", error))
        .subscribe();
  }
}
