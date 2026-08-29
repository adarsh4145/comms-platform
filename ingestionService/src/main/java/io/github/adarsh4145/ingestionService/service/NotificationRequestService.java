package io.github.adarsh4145.ingestionService.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import io.github.adarsh4145.ingestionService.domain.OutboxEvent;
import io.github.adarsh4145.ingestionService.pojo.CreateNotificationRequest;
import io.github.adarsh4145.ingestionService.repository.NotificationRequestRepository;
import io.github.adarsh4145.ingestionService.repository.OutboxEventRepository;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Service
public class NotificationRequestService {

  private static final String IDEMPOTENCY_KEY_PREFIX = "idempotency:notification:";
  private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

  private final NotificationRequestRepository notificationRequestRepository;
  private final OutboxEventRepository outboxEventRepository;
  private final TransactionalOperator transactionalOperator;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final ObjectMapper objectMapper;

  public Mono<NotificationRequest> createNotification(String idempotencyKey, CreateNotificationRequest request) {
    String redisKey = IDEMPOTENCY_KEY_PREFIX + idempotencyKey;

    return redisTemplate.opsForValue().get(redisKey)
            .flatMap(existingNotificationId ->
                    notificationRequestRepository.findById(existingNotificationId))
            .switchIfEmpty(Mono.defer(() -> processNewNotification(redisKey, request)));
  }

  private Mono<NotificationRequest> processNewNotification(String redisKey, CreateNotificationRequest request) {
    NotificationRequest notification = NotificationRequest.builder()
            .recipient(request.recipient())
            .message(request.message())
            .priority(request.priority())
            .status(NotificationRequest.Status.RECEIVED)
            .createdAt(Instant.now())
            .build();

    Mono<NotificationRequest> flow = notificationRequestRepository.save(notification)
            .flatMap(savedNotification -> {
              OutboxEvent outboxEvent = OutboxEvent.builder()
                      .aggregateId(savedNotification.getId())
                      .eventType("NotificationCreated")
                      .priority(savedNotification.getPriority())
                      .payload(toJson(savedNotification))
                      .status(OutboxEvent.Status.PENDING)
                      .createdAt(Instant.now())
                      .build();

              return outboxEventRepository.save(outboxEvent)
                      .then(redisTemplate.opsForValue().set(redisKey, savedNotification.getId(), IDEMPOTENCY_TTL))
                      .thenReturn(savedNotification);
            });

    return transactionalOperator.transactional(flow);
  }

  @SneakyThrows
  private String toJson(NotificationRequest notification) {
    return objectMapper.writeValueAsString(
        Map.of(
            "requestId", notification.getId(),
            "recipient", notification.getRecipient(),
            "message", notification.getMessage(),
            "priority", notification.getPriority().name()));
  }
}
