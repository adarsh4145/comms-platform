package io.github.adarsh4145.ingestionService.service;

import io.github.adarsh4145.core.tracing.TracePropagation;
import io.github.adarsh4145.ingestionService.domain.OutboxEvent;
import io.github.adarsh4145.ingestionService.kafka.OutboxEventValidator;
import io.github.adarsh4145.ingestionService.kafka.publisher.NotificationEventPublisher;
import io.github.adarsh4145.ingestionService.repository.OutboxEventClaimer;
import io.github.adarsh4145.ingestionService.repository.OutboxEventRepository;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

  private static final String RELAY_SPAN_NAME = "outbox.relay.publish";

  /** Upper bound on rows handled per pass, so one pass cannot run unboundedly long. */
  private static final int MAX_BATCH = 100;

  /** A claim older than this is assumed to belong to a relay that died. */
  private static final Duration STALE_CLAIM_AFTER = Duration.ofMinutes(2);

  private final OutboxEventRepository outboxEventRepository;
  private final OutboxEventClaimer outboxEventClaimer;
  private final NotificationEventPublisher notificationEventPublisher;
  private final OutboxEventValidator outboxEventValidator;
  private final TracePropagation tracePropagation;
  private final NotificationStatusService notificationStatusService;

  /**
   * {@code @Scheduled} measures fixedDelay from method return, and these methods return the moment
   * they subscribe. Without these guards a slow pass would be re-entered every 5 seconds.
   */
  /**
   * fixedDelay only spaces runs apart; the first one still fires the moment the context is up.
   * Tests push this out so the relay never reaches for a database they did not ask for.
   */
  private static final String INITIAL_DELAY = "${ingestion.outbox.initial-delay-ms:0}";

  private final AtomicBoolean pendingPassRunning = new AtomicBoolean();

  private final AtomicBoolean retryPassRunning = new AtomicBoolean();

  // Delays are configurable so the context test can push them past its own lifetime. Left as
  // literals they fired 5s after startup, against a database the test has no business needing,
  // and buried the build output in Mongo auth stack traces that no one was meant to act on.
  @Scheduled(
      fixedDelayString = "${ingestion.outbox.relay-delay-ms:5000}",
      initialDelayString = INITIAL_DELAY)
  public void relayPendingEvents() {
    runPass("relay", pendingPassRunning, Set.of(OutboxEvent.Status.PENDING));
  }

  @Scheduled(
      fixedDelayString = "${ingestion.outbox.retry-delay-ms:30000}",
      initialDelayString = INITIAL_DELAY)
  public void retryFailedEvents() {
    runPass("retry", retryPassRunning, Set.of(OutboxEvent.Status.FAILED));
  }

  @Scheduled(
      fixedDelayString = "${ingestion.outbox.reclaim-delay-ms:60000}",
      initialDelayString = INITIAL_DELAY)
  public void reclaimStaleClaims() {
    outboxEventClaimer
        .reclaimStaleClaims(STALE_CLAIM_AFTER)
        .doOnError(error -> log.error("Failed reclaiming stale outbox claims", error))
        .onErrorComplete()
        .subscribe();
  }

  private void runPass(
      String passName, AtomicBoolean guard, Collection<OutboxEvent.Status> claimable) {
    if (!guard.compareAndSet(false, true)) {
      log.debug("Outbox {} pass still in flight, skipping this tick", passName);
      return;
    }
    drain(claimable, MAX_BATCH)
        .doOnError(error -> log.error("Unexpected error in outbox {} pass", passName, error))
        .onErrorComplete()
        .doFinally(signal -> guard.set(false))
        .subscribe();
  }

  /**
   * Claims and processes one row at a time until nothing is claimable or the batch cap is hit. An
   * empty claim completes the chain, so a drained outbox costs exactly one findAndModify.
   */
  private Mono<Void> drain(Collection<OutboxEvent.Status> claimable, int remaining) {
    if (remaining <= 0) {
      return Mono.empty();
    }
    return outboxEventClaimer
        .claimNext(claimable)
        .flatMap(event -> processEvent(event).then(drain(claimable, remaining - 1)))
        .then();
  }

  private Mono<OutboxEvent> processEvent(OutboxEvent event) {
    List<String> violations = outboxEventValidator.findViolations(event);
    if (!violations.isEmpty()) {
      log.warn(
          "Outbox event {} is malformed, marking as MALFORMED (will not be retried): {}",
          event.getId(),
          violations);
      event.setStatus(OutboxEvent.Status.MALFORMED);
      return outboxEventRepository.save(event);
    }

    // StreamBridge.send blocks, so it must not run on the Mongo event loop.
    return Mono.fromCallable(() -> publishInOriginalTrace(event))
        .subscribeOn(Schedulers.boundedElastic())
        .map(
            status -> {
              event.setStatus(status);
              return event;
            })
        .flatMap(outboxEventRepository::save)
        // Once the event is on Kafka the request is genuinely in flight, so reflect that on the
        // notification. The terminal SENT/FAILED comes back later from dispatchService.
        .flatMap(
            saved ->
                saved.getStatus() == OutboxEvent.Status.PUBLISHED
                    ? notificationStatusService
                        .markProcessing(saved.getNotificationId())
                        .thenReturn(saved)
                    : Mono.just(saved));
  }

  /**
   * Re-opens the trace context that was captured when the row was written, so the Kafka producer
   * span (and everything downstream of it) hangs off the original request instead of starting a
   * fresh trace on this scheduler thread.
   */
  private OutboxEvent.Status publishInOriginalTrace(OutboxEvent event) {
    return tracePropagation.continueTrace(
        event.getTraceContext(),
        RELAY_SPAN_NAME,
        () -> {
          try {
            boolean sent =
                notificationEventPublisher.publish(
                    event.getPriority(), event.getEventType(), event.getId(), event.getPayload());
            log.info(
                "Relayed outbox event {} for notification {} on attempt {} -> sent={}",
                event.getId(),
                event.getNotificationId(),
                event.getAttempts(),
                sent);
            return sent ? OutboxEvent.Status.PUBLISHED : OutboxEvent.Status.FAILED;
          } catch (Exception ex) {
            log.error(
                "Transient failure relaying outbox event {} on attempt {}, will retry",
                event.getId(),
                event.getAttempts(),
                ex);
            return OutboxEvent.Status.FAILED;
          }
        });
  }
}
