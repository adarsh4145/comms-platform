package io.github.adarsh4145.dispatchService.service;

import io.github.adarsh4145.core.event.NotificationCreatedEvent;
import io.github.adarsh4145.core.tracing.TracePropagation;
import io.github.adarsh4145.dispatchService.domain.DeadLetterEvent;
import io.github.adarsh4145.dispatchService.repository.DeadLetterEventRepository;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadLetterService {

  /** Enough to identify the failure without storing an unbounded blob per row. */
  private static final int MAX_FAILURE_DETAIL = 8000;

  private final DeadLetterEventRepository deadLetterEventRepository;
  private final TracePropagation tracePropagation;

  @Transactional
  public DeadLetterEvent record(
      String priorityLabel,
      NotificationCreatedEvent event,
      String payload,
      DeadLetterEvent.FailureReason reason,
      Throwable failure,
      int attempts) {

    DeadLetterEvent row =
        DeadLetterEvent.builder()
            .id(UUID.randomUUID())
            .eventId(event == null ? null : event.getEventId())
            .notificationId(event == null ? null : event.getRequestId())
            .priority(event == null ? priorityLabel : event.getPriority())
            .channel(event == null || event.getChannel() == null ? null : event.getChannel().name())
            .recipient(event == null ? null : event.getRecipient())
            .payload(payload)
            .traceId(tracePropagation.currentTraceId())
            .failureReason(reason)
            .failureDetail(describe(failure))
            .attempts(attempts)
            .createdAt(Instant.now())
            .status(DeadLetterEvent.Status.NEW)
            .build();

    DeadLetterEvent saved = deadLetterEventRepository.save(row);
    log.error(
        "Dead-lettered notification {} ({} after {} attempt(s)) as DLQ row {}",
        saved.getNotificationId(),
        reason,
        attempts,
        saved.getId());
    return saved;
  }

  @Transactional(readOnly = true)
  public List<DeadLetterEvent> findRecent(DeadLetterEvent.Status status, int limit) {
    return deadLetterEventRepository.findByStatusOrderByCreatedAtDesc(
        status, PageRequest.of(0, limit));
  }

  @Transactional(readOnly = true)
  public Optional<DeadLetterEvent> findById(UUID id) {
    return deadLetterEventRepository.findById(id);
  }

  @Transactional(readOnly = true)
  public List<DeadLetterEvent> findByTraceId(String traceId) {
    return deadLetterEventRepository.findByTraceId(traceId);
  }

  @Transactional
  public void markReplayed(UUID id) {
    deadLetterEventRepository
        .findById(id)
        .ifPresent(
            row -> {
              row.setStatus(DeadLetterEvent.Status.REPLAYED);
              row.setLastReplayedAt(Instant.now());
              deadLetterEventRepository.save(row);
            });
  }

  @Transactional
  public void markDiscarded(UUID id) {
    deadLetterEventRepository
        .findById(id)
        .ifPresent(
            row -> {
              row.setStatus(DeadLetterEvent.Status.DISCARDED);
              deadLetterEventRepository.save(row);
            });
  }

  private String describe(Throwable failure) {
    if (failure == null) {
      return null;
    }
    StringWriter writer = new StringWriter();
    failure.printStackTrace(new PrintWriter(writer));
    String detail = writer.toString();
    return detail.length() <= MAX_FAILURE_DETAIL
        ? detail
        : detail.substring(0, MAX_FAILURE_DETAIL) + "\n... (truncated)";
  }
}
