package io.github.adarsh4145.ingestionService.domain;

import io.github.adarsh4145.core.provider.SendRequest;
import java.time.Instant;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "notification_requests")
@ToString
public class NotificationRequest {

  @Id private String id;

  private String recipient;

  private String from;

  private String message;

  private Priority priority;

  private Status status;

  private Instant createdAt;

  private SendRequest.Channel channel;

  /** When status last changed. Everything below is filled in by the delivery-status feedback. */
  private Instant statusUpdatedAt;

  private Integer deliveryAttempts;

  private String providerMessageId;

  private String deliveryError;

  public enum Priority {
    CRITICAL,
    HIGH,
    MEDIUM,
    LOW
  }

  public enum Status {
    RECEIVED,
    PROCESSING,
    SENT,
    FAILED
  }
}
