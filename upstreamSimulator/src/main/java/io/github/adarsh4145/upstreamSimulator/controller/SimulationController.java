package io.github.adarsh4145.upstreamSimulator.controller;

import io.github.adarsh4145.upstreamSimulator.dto.SimulationRequest;
import io.github.adarsh4145.upstreamSimulator.dto.SimulationResponse;
import io.github.adarsh4145.upstreamSimulator.service.SimulatorService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping(value = "/simulate")
@Slf4j
@RequiredArgsConstructor
public class SimulationController {

  private final SimulatorService simulatorService;

  /**
   * Returns the {@code Mono} directly. Wrapping it in a {@code ResponseEntity<Object>} made the
   * declared body type {@code Object}, so the publisher was never subscribed and the request hung.
   */
  @PostMapping("/notification")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public Mono<SimulationResponse> simulateNotification(
      @Valid @RequestBody SimulationRequest simulationRequest) {
    log.info(
        "recipient: {}, message: {}, priority: {}, channel: {}, from: {}",
        simulationRequest.getRecipient(),
        simulationRequest.getMessage(),
        simulationRequest.getPriority(),
        simulationRequest.getChannel(),
        simulationRequest.getFrom());

    return simulatorService.simulate(simulationRequest);
  }
}
