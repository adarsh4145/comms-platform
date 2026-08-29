package io.github.adarsh4145.ingestionService.repository;

import io.github.adarsh4145.ingestionService.domain.OutboxEvent;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface OutboxEventRepository extends ReactiveMongoRepository<OutboxEvent, String> {
  Flux<OutboxEvent> findByStatus(OutboxEvent.Status status);
}
