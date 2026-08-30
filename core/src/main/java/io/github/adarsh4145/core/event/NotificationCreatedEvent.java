package io.github.adarsh4145.core.event;

public record NotificationCreatedEvent(
        String requestId,
        String recipient,
        String message,
        String priority
) {}
