package io.github.adarsh4145.ingestionService.domain;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "notification_requests")
public class NotificationRequest {

  @Id private String id;

  private String recipient;

  private String message;

  private Priority priority;

  private Status status;

  private Instant createdAt;

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
