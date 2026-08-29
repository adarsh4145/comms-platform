package io.github.adarsh4145.ingestionService.pojo;

import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateNotificationRequest(
        @NotBlank String recipient,
        @NotBlank String message,
        @NotNull NotificationRequest.Priority priority
) {}