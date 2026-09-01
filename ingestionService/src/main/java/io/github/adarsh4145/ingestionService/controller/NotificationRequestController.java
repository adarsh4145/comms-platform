package io.github.adarsh4145.ingestionService.controller;

import io.github.adarsh4145.core.ingestion.CreateNotificationRequest;
import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import io.github.adarsh4145.ingestionService.service.NotificationRequestService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@Slf4j
public class NotificationRequestController {

  private final NotificationRequestService notificationRequestService;

  @PostMapping("/notifications")
  @ResponseStatus(HttpStatus.CREATED)
  public Mono<NotificationRequest> createNotification(
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @Valid @RequestBody CreateNotificationRequest request) {
    log.info("request received: {}", request);
    return notificationRequestService.createNotification(idempotencyKey, request);
  }
}
