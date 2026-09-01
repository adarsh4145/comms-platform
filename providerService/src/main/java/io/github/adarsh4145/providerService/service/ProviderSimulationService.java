package io.github.adarsh4145.providerService.service;

import io.github.adarsh4145.core.provider.SendRequest;
import io.github.adarsh4145.core.provider.SendResponse;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProviderSimulationService {

  private final JavaMailSender mailSender;

  /**
   * Boot's auto-configured builder, not a hand-made {@code WebClient.builder()} — only the
   * auto-configured one carries the observation customizer that injects {@code traceparent} into
   * the outgoing SMS call.
   */
  private final WebClient.Builder webClientBuilder;

  private final ObservationRegistry observationRegistry;

  private WebClient webClient;

  @Value("${provider.simulation.failure-rate}")
  private double failureRate;

  @Value("${provider.simulation.min-delay-ms}")
  private long minDelayMs;

  @Value("${provider.simulation.max-delay-ms}")
  private long maxDelayMs;

  @Value("${provider.sms.gateway-url}")
  private String smsGatewayUrl;

  @Value("${provider.sms.default-sender}")
  private String defaultSmsSender;

  @Value("${provider.email.default-sender}")
  private String defaultEmailSender;

  @PostConstruct
  void init() {
    this.webClient = webClientBuilder.build();
  }

  public Mono<SendResponse> send(SendRequest request) {
    long delay = ThreadLocalRandom.current().nextLong(minDelayMs, maxDelayMs);
    boolean simulatedFailure = ThreadLocalRandom.current().nextDouble() < failureRate;

    return Mono.delay(Duration.ofMillis(delay))
        .then(
            Mono.defer(
                () -> {
                  if (simulatedFailure) {
                    log.warn("Simulated provider failure for recipient: {}", request.recipient());
                    return Mono.just(new SendResponse(false, null, "Simulated provider failure"));
                  }
                  return switch (request.channel()) {
                    case EMAIL -> sendEmail(request);
                    case SMS -> sendSms(request);
                  };
                }));
  }

  private Mono<SendResponse> sendEmail(SendRequest request) {
    return Mono.fromCallable(
            () ->
                // JavaMailSender has no built-in instrumentation, so the SMTP hop only shows up
                // in the trace if we open a span for it by hand.
                Observation.createNotStarted("provider.send", observationRegistry)
                    .lowCardinalityKeyValue("provider.channel", "EMAIL")
                    .observe(
                        () -> {
                          SimpleMailMessage mail = new SimpleMailMessage();
                          mail.setFrom(
                              request.from() != null ? request.from() : defaultEmailSender);
                          mail.setTo(request.recipient());
                          mail.setSubject(request.subject());
                          mail.setText(request.message());
                          log.info("sending mail: {}", request);
                          mailSender.send(mail);
                          return new SendResponse(true, UUID.randomUUID().toString(), null);
                        }))
        .doOnSuccess(sendResponse -> log.info("Sent, {}", sendResponse.providerMessageId()))
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(
            ex -> {
              log.error("Email delivery failed", ex);
              return Mono.just(new SendResponse(false, null, ex.getMessage()));
            });
  }

  private Mono<SendResponse> sendSms(SendRequest request) {
    String sender = request.from() != null ? request.from() : defaultSmsSender;

    return webClient
        .post()
        .uri(smsGatewayUrl)
        .bodyValue(
            Map.of(
                "MessageSid", UUID.randomUUID().toString(),
                "From", sender,
                "To", request.recipient(),
                "Body", request.message()))
        .retrieve()
        .toBodilessEntity()
        .map(
            response -> {
              log.info("SMS sent successfully: {}", response);
              return new SendResponse(true, UUID.randomUUID().toString(), null);
            })
        .onErrorResume(
            ex -> {
              log.error("SMS delivery failed", ex);
              return Mono.just(new SendResponse(false, null, ex.getMessage()));
            });
  }
}
