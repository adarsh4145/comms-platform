package io.github.adarsh4145.core.tracing;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Stops Prometheus' own scrapes from being recorded as application traffic.
 *
 * <p>Every service is scraped every 15 seconds, and an inbound request is an observation - which
 * feeds a span in Tempo, an {@code http_server_requests} timeseries, and the RED panels built on
 * it, all at once. Across seven services that is roughly 40,000 spans a day whose entire content is
 * "the monitoring system asked me for metrics", and it was the only traffic most RED panels ever
 * saw, so they described the scraper rather than the application.
 *
 * <p>Moving actuator to its own port does not help - the management context registers its own
 * observation filter, so the requests are still recorded. Vetoing the observation is what actually
 * silences it, and it silences the span and the metric together because they share one source.
 *
 * <p>The endpoints keep working exactly as before; only the recording of the request that hit them
 * goes away. Prometheus' own {@code up} and {@code scrape_duration_seconds} remain the better
 * signal for whether scraping itself is healthy.
 */
@AutoConfiguration
public class CoreObservationAutoConfiguration {

  private static final String ACTUATOR_PREFIX = "/actuator/";

  /**
   * Reactive and servlet requests arrive in two different {@code ServerRequestObservationContext}
   * classes that share no common supertype worth matching on, so they get one predicate each.
   * Splitting them across nested configurations keeps the servlet class from being loaded at all in
   * a WebFlux service, where {@code jakarta.servlet} is not on the classpath.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(org.springframework.http.server.reactive.observation.ServerRequestObservationContext.class)
  static class Reactive {

    @Bean
    ObservationPredicate skipActuatorReactiveRequests() {
      return (name, context) ->
          !(context
                  instanceof
                  org.springframework.http.server.reactive.observation
                          .ServerRequestObservationContext ctx)
              || !ctx.getCarrier().getPath().value().startsWith(ACTUATOR_PREFIX);
    }
  }

  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(jakarta.servlet.http.HttpServletRequest.class)
  static class Servlet {

    @Bean
    ObservationPredicate skipActuatorServletRequests() {
      return (name, context) ->
          !(context
                  instanceof
                  org.springframework.http.server.observation.ServerRequestObservationContext ctx)
              || !ctx.getCarrier().getRequestURI().startsWith(ACTUATOR_PREFIX);
    }
  }
}
