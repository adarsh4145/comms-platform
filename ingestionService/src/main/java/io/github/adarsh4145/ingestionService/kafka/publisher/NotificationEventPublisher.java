package io.github.adarsh4145.ingestionService.kafka.publisher;

import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import io.github.adarsh4145.ingestionService.kafka.NotificationTopics;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class NotificationEventPublisher {

  private final StreamBridge streamBridge;

  public boolean publish(NotificationRequest.Priority priority, String eventType, String payload) {
    String topic = NotificationTopics.forPriority(priority);
    return streamBridge.send(topic, payload);
  }
}
