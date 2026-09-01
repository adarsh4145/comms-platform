package io.github.adarsh4145.dispatchService.consumer;

import io.github.adarsh4145.dispatchService.service.NotificationDispatchService;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class NotificationConsumers {

  private final NotificationDispatchService notificationDispatchService;

  @Bean
  public Consumer<String> dispatchCritical() {
    return payload -> handle("CRITICAL", payload);
  }

  @Bean
  public Consumer<String> dispatchHigh() {
    return payload -> handle("HIGH", payload);
  }

  @Bean
  public Consumer<String> dispatchMedium() {
    return payload -> handle("MEDIUM", payload);
  }

  @Bean
  public Consumer<String> dispatchLow() {
    return payload -> handle("LOW", payload);
  }

  /**
   * Runs inside the binder's consumer observation (see {@code
   * spring.cloud.stream.kafka.binder.enable-observation}), so the record's {@code traceparent}
   * header is already the active context here and everything below continues the same trace.
   *
   * <p>Never rethrows: retries and the dead-letter decision both live in the dispatch service, so
   * letting an exception escape here would only add a second, uncoordinated retry loop in the
   * binder.
   */
  private void handle(String priorityLabel, String payload) {
    log.info("[{}] Received notification event: {}", priorityLabel, payload);
    notificationDispatchService.dispatch(priorityLabel, payload);
  }
}
