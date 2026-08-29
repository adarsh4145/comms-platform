package io.github.adarsh4145.ingestionService.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import io.github.adarsh4145.ingestionService.domain.OutboxEvent;
import io.github.adarsh4145.ingestionService.pojo.CreateNotificationRequest;
import io.github.adarsh4145.ingestionService.repository.NotificationRequestRepository;
import io.github.adarsh4145.ingestionService.repository.OutboxEventRepository;
import java.time.Instant;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Service
public class NotificationRequestService {
  private final NotificationRequestRepository notificationRequestRepository;
  private final OutboxEventRepository outboxEventRepository;
  private final TransactionalOperator transactionalOperator;
  private final ObjectMapper objectMapper;

  public Mono<NotificationRequest> createNotification(CreateNotificationRequest request) {

    NotificationRequest notification =
        NotificationRequest.builder()
            .recipient(request.recipient())
            .message(request.message())
            .priority(request.priority())
            .status(NotificationRequest.Status.RECEIVED)
            .createdAt(Instant.now())
            .build();

    Mono<NotificationRequest> flow =
        notificationRequestRepository
            .save(notification)
            .flatMap(
                savedNotification -> {
                  OutboxEvent outboxEvent =
                      OutboxEvent.builder()
                          .aggregateId(savedNotification.getId())
                          .eventType("NotificationCreated")
                          .payload(toJson(savedNotification))
                          .priority(request.priority())
                          .status(OutboxEvent.Status.PENDING)
                          .createdAt(Instant.now())
                          .build();

                  return outboxEventRepository.save(outboxEvent).thenReturn(savedNotification);
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
