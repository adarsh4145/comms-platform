package io.github.adarsh4145.dispatchService.publisher;

import io.github.adarsh4145.core.event.EventSerde;
import io.github.adarsh4145.core.event.NotificationCreatedEvent;
import io.github.adarsh4145.core.event.NotificationDeliveryEvent;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * Reports terminal delivery outcomes back to ingestionService over Kafka.
 *
 * <p>Published from inside the consumer observation, so the binder injects {@code traceparent} and
 * the status update stays in the same trace as the delivery it describes.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DeliveryStatusPublisher {

  private static final String EVENT_TYPE = "NotificationDelivered";
  private static final String SOURCE_IDENTIFIER = "dispatchService";

  private final StreamBridge streamBridge;
  private final EventSerde eventSerde;

  @Value("${dispatch.delivery.status-topic:notification.delivery.status}")
  private String statusTopic;

  public void publish(
      NotificationCreatedEvent source,
      NotificationDeliveryEvent.DeliveryStatus status,
      String providerMessageId,
      String errorMessage,
      int attempts) {

    NotificationDeliveryEvent event =
        NotificationDeliveryEvent.builder()
            .eventId(UUID.randomUUID().toString())
            .eventType(EVENT_TYPE)
            .createdAt(Instant.now())
            .sourceIdentifier(SOURCE_IDENTIFIER)
            .traceId(source.getTraceId())
            .requestId(source.getRequestId())
            .channel(source.getChannel())
            .recipient(source.getRecipient())
            .status(status)
            .providerMessageId(providerMessageId)
            .errorMessage(errorMessage)
            .attempts(attempts)
            .build();

    Message<String> message =
        MessageBuilder.withPayload(eventSerde.toJson(event))
            // Key by requestId so all status updates for one notification land on one partition
            // and therefore stay ordered.
            .setHeader(KafkaHeaders.KEY, source.getRequestId())
            .build();

    boolean sent = streamBridge.send(statusTopic, message);
    if (!sent) {
      // Not fatal: the notification itself was already delivered or dead-lettered. The document
      // just keeps its previous status until something reconciles it.
      log.warn(
          "Failed publishing delivery status {} for notification {}",
          status,
          source.getRequestId());
    }
  }
}
