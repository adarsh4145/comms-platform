package io.github.adarsh4145.ingestionService.kafka.publisher;

import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import io.github.adarsh4145.ingestionService.kafka.NotificationTopics;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class NotificationEventPublisher {

  private final StreamBridge streamBridge;

  public boolean publish(
      NotificationRequest.Priority priority, String eventType, String eventId, String payload) {
    String topic = NotificationTopics.forPriority(priority);
    Message<String> message =
        MessageBuilder.withPayload(payload).setHeader(KafkaHeaders.KEY, eventId).build();
    return streamBridge.send(topic, message);
  }
}
