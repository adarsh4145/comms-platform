package io.github.adarsh4145.upstreamSimulator.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Builder
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SimulationRequest {

  @NotBlank private String recipient;

  @NotBlank private String message;

  @NotNull private Priority priority;

  @NotNull private Channel channel;

  @NotBlank private String from;

  public enum Priority {
    CRITICAL,
    HIGH,
    MEDIUM,
    LOW
  }

  public enum Channel {
    EMAIL,
    SMS
  }

  /**
   * Basis for the idempotency key. Every field it touches is dereferenced, which is why they all
   * carry constraints - a payload missing {@code channel} used to NPE here.
   */
  public String getConcatValues() {
    return channel.toString() + priority.toString() + recipient + from + message;
  }
}
