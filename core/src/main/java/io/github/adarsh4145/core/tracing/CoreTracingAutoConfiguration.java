package io.github.adarsh4145.core.tracing;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Registered via {@code AutoConfiguration.imports} so every service depending on {@code core} picks
 * it up without component-scanning into this package.
 *
 * <p>Resolution goes through {@link ObjectProvider} deliberately: {@code @ConditionalOnBean} is
 * evaluated at configuration-registration time and would lose the race against Boot's own tracing
 * auto-configuration. Looking the beans up lazily also lets the helper degrade to a no-op when
 * tracing is switched off entirely.
 */
@AutoConfiguration
@ConditionalOnClass({Tracer.class, Propagator.class})
public class CoreTracingAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public TracePropagation tracePropagation(
      ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
    return new TracePropagation(tracer.getIfAvailable(), propagator.getIfAvailable());
  }

  /**
   * The OTLP appender in core's {@code logback-spring.xml} has no SDK to write to until someone
   * hands it one — Boot creates an {@link OpenTelemetry} bean but never registers it globally, so
   * Logback and the SDK would otherwise never meet and Loki would stay empty. Anything logged
   * before this runs is replayed from the appender's own buffer, so startup logs are not lost.
   */
  @Bean
  @ConditionalOnClass(OpenTelemetry.class)
  public InitializingBean openTelemetryLogbackAppenderInstaller(
      ObjectProvider<OpenTelemetry> openTelemetry) {
    return () -> openTelemetry.ifAvailable(OpenTelemetryAppender::install);
  }
}
