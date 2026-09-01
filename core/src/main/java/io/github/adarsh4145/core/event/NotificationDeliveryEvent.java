package io.github.adarsh4145.core.event;

import io.github.adarsh4145.core.provider.SendRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Emitted by dispatchService once a notification has reached a terminal delivery outcome.
 *
 * <p>Closes the loop that was previously open: ingestionService wrote every request as RECEIVED and
 * nothing ever told it what happened afterwards.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@SuperBuilder
public class NotificationDeliveryEvent extends CommsBaseEvent {

  /** Id of the NotificationRequest document in ingestionService. */
  private String requestId;

  private SendRequest.Channel channel;

  private String recipient;

  private DeliveryStatus status;

  private String providerMessageId;

  private String errorMessage;

  private int attempts;

  public enum DeliveryStatus {
    SENT,
    FAILED
  }
}
