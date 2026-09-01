package io.github.adarsh4145.upstreamSimulator.service;

import io.github.adarsh4145.upstreamSimulator.dto.SimulationRequest;
import io.github.adarsh4145.upstreamSimulator.dto.SimulationResponse;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class SimulatorService {

  private final WebClient webClient;

  public Mono<SimulationResponse> simulate(SimulationRequest simulationRequest) {

    var idempotencyKey =
        UUID.nameUUIDFromBytes(simulationRequest.getConcatValues().getBytes()).toString();

    // No hand-rolled Trace-Id header: the instrumented WebClient injects W3C `traceparent`,
    // which every downstream hop understands without us naming it.
    return webClient
        .post()
        .uri("")
        .header("Idempotency-Key", idempotencyKey)
        .bodyValue(simulationRequest)
        .retrieve()
        .bodyToMono(SimulationResponse.class)
        .doOnSuccess(
            simulationResponse ->
                log.info("successfully sent notification, {}", simulationResponse))
        .doOnError(
            throwable ->
                log.error("failed to send notification, error: {}", throwable.getMessage()));
  }
}
