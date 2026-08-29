package io.github.adarsh4145.ingestionService.kafka;

import io.github.adarsh4145.ingestionService.domain.OutboxEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class OutboxEventValidator {

  public boolean isValid(OutboxEvent event) {
    return event.getPriority() != null
        && StringUtils.hasText(event.getEventType())
        && StringUtils.hasText(event.getPayload());
  }
}
