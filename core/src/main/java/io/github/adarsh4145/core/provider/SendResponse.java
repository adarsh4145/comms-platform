package io.github.adarsh4145.core.provider;

public record SendResponse(boolean success, String providerMessageId, String errorMessage) {}
