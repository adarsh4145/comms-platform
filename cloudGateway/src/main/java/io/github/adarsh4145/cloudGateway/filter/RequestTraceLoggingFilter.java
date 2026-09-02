package io.github.adarsh4145.cloudGateway.filter;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * The gateway was invisible in the logs: it opens the trace for every request, but Spring Cloud
 * Gateway itself logs nothing at INFO, so there was no line carrying the id it just minted. One
 * access log per request fixes that and makes the gateway the obvious place to start when following
 * a request through Tempo.
 *
 * <p>Actuator paths are skipped. Prometheus scrapes /actuator/prometheus every 15 seconds, and
 * logging that produced roughly 5,800 lines a day of pure noise in Loki that drowned out real
 * traffic - the access log is here to show requests being proxied, not to narrate health checks.
 *
 * <p>The logging happens in {@code doFinally} rather than at entry on purpose - the response status
 * only exists once the exchange is done, and by then {@code spring.reactor.context-propagation:
 * auto} has restored the trace context onto the signalling thread, so MDC is populated.
 */
@Component
@Slf4j
public class RequestTraceLoggingFilter implements WebFilter {

  private static final String ACTUATOR_PREFIX = "/actuator/";

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    ServerHttpRequest request = exchange.getRequest();
    if (request.getURI().getRawPath().startsWith(ACTUATOR_PREFIX)) {
      return chain.filter(exchange);
    }
    long startedAt = System.nanoTime();
    return chain
        .filter(exchange)
        .doFinally(
            signal ->
                log.info(
                    "{} {} -> {} in {} ms",
                    request.getMethod(),
                    request.getURI().getRawPath(),
                    exchange.getResponse().getStatusCode(),
                    Duration.ofNanos(System.nanoTime() - startedAt).toMillis()));
  }
}
