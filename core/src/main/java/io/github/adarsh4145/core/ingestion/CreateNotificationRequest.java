package io.github.adarsh4145.core.ingestion;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * These constraints only fire if the handler argument is annotated {@code @Valid} - without it they
 * are inert and a payload missing {@code channel} reaches the service and NPEs on {@code
 * channel().toString()}.
 */
public record CreateNotificationRequest(
    @NotBlank String recipient,
    @NotBlank String message,
    @NotNull Priority priority,
    @NotNull Channel channel,
    @NotBlank String from) {

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
}
