package io.github.adarsh4145.core.provider;

public record SendRequest(
        Channel channel,
        String sender,
        String recipient,
        String subject,
        String message
) {
    public enum Channel {
        EMAIL, SMS
    }
}