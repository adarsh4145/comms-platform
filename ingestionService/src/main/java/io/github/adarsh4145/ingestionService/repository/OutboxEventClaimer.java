package io.github.adarsh4145.ingestionService.repository;

import com.mongodb.client.result.UpdateResult;
import io.github.adarsh4145.ingestionService.domain.OutboxEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Hands out outbox rows one at a time under an atomic claim.
 *
 * <p>The relay used to read rows with a plain {@code findByStatus}, which means two ingestion
 * instances read the same PENDING row and both publish it. {@code findAndModify} makes the read and
 * the status flip a single atomic operation, so exactly one instance can win a given row.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxEventClaimer {

  private static final String STATUS = "status";
  private static final String CLAIMED_AT = "claimedAt";
  private static final String CLAIMED_BY = "claimedBy";

  private final ReactiveMongoTemplate reactiveMongoTemplate;

  /** Identifies this process in the claim, so a stuck row can be attributed to an instance. */
  private final String instanceId = UUID.randomUUID().toString();

  /**
   * Atomically flips the oldest claimable row to PROCESSING and returns it. Completes empty when
   * there is nothing left to claim, which is what tells the relay to stop draining.
   */
  public Mono<OutboxEvent> claimNext(Collection<OutboxEvent.Status> claimable) {
    Query query =
        Query.query(Criteria.where(STATUS).in(claimable))
            .with(Sort.by(Sort.Direction.ASC, "createdAt"));

    Update update =
        new Update()
            .set(STATUS, OutboxEvent.Status.PROCESSING)
            .set(CLAIMED_BY, instanceId)
            .set(CLAIMED_AT, Instant.now())
            .inc("attempts", 1);

    return reactiveMongoTemplate.findAndModify(
        query, update, FindAndModifyOptions.options().returnNew(true), OutboxEvent.class);
  }

  /**
   * Returns rows whose claim has gone stale to PENDING. Without this, a relay that dies mid-publish
   * leaves its claimed rows in PROCESSING forever and the notification is never sent.
   */
  public Mono<Long> reclaimStaleClaims(Duration staleAfter) {
    Query query =
        Query.query(
            Criteria.where(STATUS)
                .is(OutboxEvent.Status.PROCESSING)
                .and(CLAIMED_AT)
                .lt(Instant.now().minus(staleAfter)));

    Update update =
        new Update().set(STATUS, OutboxEvent.Status.PENDING).unset(CLAIMED_BY).unset(CLAIMED_AT);

    return reactiveMongoTemplate
        .updateMulti(query, update, OutboxEvent.class)
        .map(UpdateResult::getModifiedCount)
        .doOnNext(
            reclaimed -> {
              if (reclaimed > 0) {
                log.warn("Reclaimed {} outbox event(s) from a stale claim", reclaimed);
              }
            });
  }
}
