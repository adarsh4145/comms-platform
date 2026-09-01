package io.github.adarsh4145.core.event;

import io.github.adarsh4145.core.provider.SendRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@SuperBuilder
public class NotificationCreatedEvent extends CommsBaseEvent {
  private String requestId;
  private String recipient;
  private String from;
  private String message;
  private String priority;
  private SendRequest.Channel channel;
}
