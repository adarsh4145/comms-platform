package io.github.adarsh4145.core;

import io.github.adarsh4145.core.event.EventSerde;
import io.github.adarsh4145.core.web.ApiExceptionHandler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.support.WebExchangeBindException;

/** Beans shared by every service that depends on {@code core}. */
@AutoConfiguration
public class CoreAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public EventSerde eventSerde() {
    return new EventSerde();
  }

  @Bean
  @ConditionalOnClass(WebExchangeBindException.class)
  @ConditionalOnMissingBean
  public ApiExceptionHandler apiExceptionHandler() {
    return new ApiExceptionHandler();
  }
}
