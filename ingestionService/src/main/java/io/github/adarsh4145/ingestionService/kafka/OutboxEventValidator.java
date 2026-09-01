package io.github.adarsh4145.ingestionService.kafka;

import io.github.adarsh4145.ingestionService.domain.OutboxEvent;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class OutboxEventValidator {

  /**
   * A row that fails this is marked MALFORMED and never retried, so the check has to cover
   * everything the relay and the downstream consumer actually dereference. It previously let
   * through rows with no notificationId or channel, which then failed on every hop instead.
   */
  public List<String> findViolations(OutboxEvent event) {
    List<String> violations = new ArrayList<>();
    if (event.getPriority() == null) {
      violations.add("priority is null (the relay cannot pick a destination topic)");
    }
    if (event.getChannel() == null) {
      violations.add("channel is null (providerService cannot pick a delivery path)");
    }
    if (!StringUtils.hasText(event.getNotificationId())) {
      violations.add("notificationId is blank (the event cannot be traced back to a request)");
    }
    if (!StringUtils.hasText(event.getEventType())) {
      violations.add("eventType is blank");
    }
    if (!StringUtils.hasText(event.getPayload())) {
      violations.add("payload is blank");
    }
    return violations;
  }

  public boolean isValid(OutboxEvent event) {
    return findViolations(event).isEmpty();
  }
}
