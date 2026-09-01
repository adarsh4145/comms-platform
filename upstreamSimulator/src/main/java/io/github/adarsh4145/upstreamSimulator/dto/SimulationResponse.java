package io.github.adarsh4145.upstreamSimulator.dto;

import io.github.adarsh4145.core.provider.SendRequest;
import java.time.Instant;
import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class SimulationResponse {

  private String id;

  private String recipient;

  private String from;

  private String message;

  private Priority priority;

  private Status status;

  private Instant createdAt;

  private SendRequest.Channel channel;

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
