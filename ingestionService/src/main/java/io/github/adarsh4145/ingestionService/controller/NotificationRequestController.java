package io.github.adarsh4145.ingestionService.controller;

import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import io.github.adarsh4145.ingestionService.pojo.CreateNotificationRequest;
import io.github.adarsh4145.ingestionService.service.NotificationRequestService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
public class NotificationRequestController {

  private final NotificationRequestService notificationRequestService;

  @PostMapping("/notifications")
  @ResponseStatus(HttpStatus.CREATED)
  public Mono<NotificationRequest> createNotification(
          @RequestHeader("Idempotency-Key") String idempotencyKey,
          @RequestBody CreateNotificationRequest request) {
    return notificationRequestService.createNotification(idempotencyKey, request);
  }
}
