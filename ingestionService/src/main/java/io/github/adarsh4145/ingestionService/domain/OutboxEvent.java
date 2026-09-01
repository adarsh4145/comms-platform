package io.github.adarsh4145.ingestionService.domain;

import io.github.adarsh4145.core.provider.SendRequest;
import java.time.Instant;
import java.util.Map;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "outbox_events")
@ToString
public class OutboxEvent {

  @Id private String id;

  private String notificationId;

  private String eventType;

  private SendRequest.Channel channel;

  private NotificationRequest.Priority priority;

  private String payload;

  private Status status;

  private Instant createdAt;

  private String sourceIdentifier;

  /** Trace id of the request that produced this row — for log/Tempo lookups by eye. */
  private String traceId;

  /**
   * Full propagation carrier (W3C {@code traceparent}/{@code tracestate}) captured while the
   * producing request was still in flight. The relay re-opens it so the Kafka publish lands in the
   * same trace instead of starting a fresh one off the scheduler thread.
   */
  private Map<String, String> traceContext;

  /** Relay instance that currently owns this row; null unless status is PROCESSING. */
  private String claimedBy;

  /** When the claim was taken, so a claim abandoned by a crashed relay can be reclaimed. */
  private Instant claimedAt;

  /** How many times the relay has attempted to publish this row. */
  private int attempts;

  public enum Status {
    PENDING,
    /** Claimed by one relay instance. Exists so no other instance picks the same row up. */
    PROCESSING,
    PUBLISHED,
    FAILED,
    MALFORMED
  }
}
