package io.github.adarsh4145.dispatchService.exception;

/** Raised when providerService reports {@code success=false} rather than throwing. */
public class ProviderDeliveryException extends RuntimeException {

  public ProviderDeliveryException(String message) {
    super(message == null ? "providerService reported an unsuccessful delivery" : message);
  }
}
