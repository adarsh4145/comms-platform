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
@Document(collection = "outbox_events")
public class OutboxEvent {

  @Id private String id;

  private String aggregateId;

  private String eventType;

  private NotificationRequest.Priority priority;

  private String payload;

  private Status status;

  private Instant createdAt;

  public enum Status {
    PENDING,
    PUBLISHED,
    FAILED,
    MALFORMED
  }
}
