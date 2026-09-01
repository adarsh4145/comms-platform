package io.github.adarsh4145.dispatchService.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * A notification that dispatchService could not deliver.
 *
 * <p>Before this existed, an exhausted delivery was logged and the Kafka offset committed anyway,
 * so the notification was simply gone. The payload is stored verbatim so a replay needs nothing but
 * this row.
 */
@Entity
@Table(
    name = "dead_letter_event",
    indexes = {
      // Column names, not field names - the default naming strategy snake_cases them.
      @Index(name = "idx_dle_status_created", columnList = "status,created_at"),
      @Index(name = "idx_dle_trace_id", columnList = "trace_id")
    })
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString(exclude = "failureDetail")
public class DeadLetterEvent {

  @Id private UUID id;

  private String eventId;

  private String notificationId;

  private String priority;

  private String channel;

  private String recipient;

  @Column(columnDefinition = "text")
  private String payload;

  /** Trace the failure belongs to - paste it into Grafana to see the whole journey. */
  private String traceId;

  @Enumerated(EnumType.STRING)
  private FailureReason failureReason;

  @Column(columnDefinition = "text")
  private String failureDetail;

  private int attempts;

  private Instant createdAt;

  private Instant lastReplayedAt;

  @Enumerated(EnumType.STRING)
  private Status status;

  public enum FailureReason {
    /** The Kafka payload could not be parsed. Replaying it will never help. */
    UNPARSEABLE_PAYLOAD,
    /** providerService was reached but reported or threw a delivery failure. */
    DELIVERY_FAILED
  }

  public enum Status {
    NEW,
    REPLAYED,
    DISCARDED
  }
}
