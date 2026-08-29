package io.github.adarsh4145.upstreamSimulator.controller;

import io.github.adarsh4145.upstreamSimulator.dto.NotificationRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/simulate")
@Slf4j
public class SimulationController {

  @PostMapping("/notification")
  public ResponseEntity<Object> simulateNotification(
      @RequestBody NotificationRequest notificationRequest) {
    log.info(
        "recipient: {}, message: {}, priority: {}",
        notificationRequest.getRecipient(),
        notificationRequest.getMessage(),
        notificationRequest.getPriority());
    return ResponseEntity.accepted().build();
  }
}
