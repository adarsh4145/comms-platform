package io.github.adarsh4145.core.web;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebInputException;

/**
 * Turns a rejected payload into a 400 that names the offending fields. The default WebFlux error
 * handler answers a bare 400 with no detail, which is useless when the caller is another service.
 *
 * <p>Lives in {@code core} and is contributed by auto-configuration so every edge service gets the
 * same behaviour without copying the class.
 */
@RestControllerAdvice
@Slf4j
public class ApiExceptionHandler {

  @ExceptionHandler(WebExchangeBindException.class)
  public ProblemDetail handleValidationFailure(WebExchangeBindException ex) {
    Map<String, String> fieldErrors = new LinkedHashMap<>();
    for (FieldError error : ex.getFieldErrors()) {
      fieldErrors.put(error.getField(), error.getDefaultMessage());
    }
    ex.getGlobalErrors()
        .forEach(error -> fieldErrors.put(error.getObjectName(), error.getDefaultMessage()));

    log.warn("rejected request: {}", fieldErrors);

    ProblemDetail problem =
        ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Request validation failed");
    problem.setTitle("Invalid request");
    problem.setProperty("errors", fieldErrors);
    return problem;
  }

  /** Covers an unreadable body and an unparseable enum value, both of which arrive as this. */
  @ExceptionHandler(ServerWebInputException.class)
  public ProblemDetail handleUnreadableInput(ServerWebInputException ex) {
    log.warn("unreadable request: {}", ex.getReason());
    ProblemDetail problem =
        ProblemDetail.forStatusAndDetail(
            HttpStatus.BAD_REQUEST,
            ex.getReason() == null ? "Request body could not be read" : ex.getReason());
    problem.setTitle("Malformed request");
    return problem;
  }
}
