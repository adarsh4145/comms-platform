package io.github.adarsh4145.upstreamSimulator.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

  @Value("${comms-platform.services.ingestionServiceUrl}")
  private String ingestionServiceUrl;

  /**
   * Built from Boot's auto-configured builder on purpose — that is what carries the observation
   * customizer that injects the W3C {@code traceparent} header. A bare {@code WebClient.builder()}
   * produces an untraced client and silently breaks the trace at the first hop.
   */
  @Bean
  public WebClient reactiveServiceClient(WebClient.Builder builder) {
    return builder.baseUrl(ingestionServiceUrl).build();
  }
}
