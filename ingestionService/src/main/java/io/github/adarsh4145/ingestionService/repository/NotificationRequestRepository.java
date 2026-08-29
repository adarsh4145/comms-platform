package io.github.adarsh4145.ingestionService.repository;

import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface NotificationRequestRepository extends ReactiveMongoRepository<NotificationRequest, String> {
}
