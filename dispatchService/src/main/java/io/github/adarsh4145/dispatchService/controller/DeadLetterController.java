package io.github.adarsh4145.dispatchService.controller;

import io.github.adarsh4145.dispatchService.domain.DeadLetterEvent;
import io.github.adarsh4145.dispatchService.service.DeadLetterService;
import io.github.adarsh4145.dispatchService.service.NotificationDispatchService;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * A dead-letter queue nobody can look at is just a slower way of losing data.
 *
 * <p>JPA is blocking and this is a WebFlux application, so every handler is pushed onto the
 * bounded-elastic scheduler rather than run on an event-loop thread.
 */
@RestController
@RequestMapping("/dlq")
@RequiredArgsConstructor
@Slf4j
public class DeadLetterController {

  private final DeadLetterService deadLetterService;
  private final NotificationDispatchService notificationDispatchService;

  @GetMapping
  public Mono<List<DeadLetterEvent>> list(
      @RequestParam(defaultValue = "NEW") DeadLetterEvent.Status status,
      @RequestParam(defaultValue = "50") int limit) {
    return Mono.fromCallable(() -> deadLetterService.findRecent(status, Math.min(limit, 500)))
        .subscribeOn(Schedulers.boundedElastic());
  }

  @GetMapping("/by-trace/{traceId}")
  public Mono<List<DeadLetterEvent>> byTrace(@PathVariable String traceId) {
    return Mono.fromCallable(() -> deadLetterService.findByTraceId(traceId))
        .subscribeOn(Schedulers.boundedElastic());
  }

  /**
   * Re-runs the original delivery through the exact same path the consumer uses. A replay that
   * fails again writes a fresh DLQ row, so the failure history is not overwritten.
   */
  @PostMapping("/{id}/replay")
  public Mono<ResponseEntity<String>> replay(@PathVariable UUID id) {
    return Mono.fromCallable(() -> doReplay(id)).subscribeOn(Schedulers.boundedElastic());
  }

  @PostMapping("/{id}/discard")
  public Mono<ResponseEntity<String>> discard(@PathVariable UUID id) {
    return Mono.fromCallable(
            () -> {
              deadLetterService.markDiscarded(id);
              return ResponseEntity.ok("discarded " + id);
            })
        .subscribeOn(Schedulers.boundedElastic());
  }

  private ResponseEntity<String> doReplay(UUID id) {
    DeadLetterEvent row = deadLetterService.findById(id).orElse(null);

    if (row == null) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND).body("no dead-letter row with id " + id);
    }
    if (row.getStatus() != DeadLetterEvent.Status.NEW) {
      return ResponseEntity.status(HttpStatus.CONFLICT)
          .body("row " + id + " is already " + row.getStatus());
    }
    if (row.getFailureReason() == DeadLetterEvent.FailureReason.UNPARSEABLE_PAYLOAD) {
      return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
          .body("row " + id + " has an unparseable payload; replaying it cannot succeed");
    }

    boolean delivered =
        notificationDispatchService.dispatch(
            row.getPriority() == null ? "REPLAY" : row.getPriority(), row.getPayload());

    if (delivered) {
      deadLetterService.markReplayed(id);
      return ResponseEntity.ok("replayed " + id);
    }
    return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
        .body("replay of " + id + " failed again; a new dead-letter row was written");
  }
}
