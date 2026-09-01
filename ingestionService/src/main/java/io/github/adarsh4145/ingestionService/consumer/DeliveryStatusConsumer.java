package io.github.adarsh4145.ingestionService.consumer;

import io.github.adarsh4145.core.event.EventSerde;
import io.github.adarsh4145.core.event.NotificationDeliveryEvent;
import io.github.adarsh4145.ingestionService.service.NotificationStatusService;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Closes the delivery loop: dispatchService reports the terminal outcome and the originating
 * NotificationRequest finally moves off RECEIVED.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class DeliveryStatusConsumer {

  private final EventSerde eventSerde;
  private final NotificationStatusService notificationStatusService;

  @Bean
  public Consumer<String> notificationDeliveryStatus() {
    return payload -> {
      try {
        NotificationDeliveryEvent event =
            eventSerde.fromJson(payload, NotificationDeliveryEvent.class);
        notificationStatusService.applyDeliveryOutcome(event);
      } catch (Exception ex) {
        // Swallowed on purpose: a bad status message must not block the partition. The
        // notification keeps its previous status, which is recoverable; a stuck consumer is not.
        log.error("Failed applying delivery status event: {}", payload, ex);
      }
    };
  }
}
