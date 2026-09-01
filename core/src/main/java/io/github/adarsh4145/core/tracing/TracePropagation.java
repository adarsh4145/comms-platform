package io.github.adarsh4145.core.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Carries a trace across an asynchronous store-and-forward hop.
 *
 * <p>The transactional outbox breaks the in-process trace: the span that writes the outbox row has
 * ended long before the relay picks that row up. Capturing the propagation headers at write time
 * and re-opening them at relay time keeps both halves in a single trace.
 *
 * <p>Degrades to a no-op when tracing is disabled, so callers never need to null-check.
 */
public class TracePropagation {

  public static final String TRACEPARENT = "traceparent";

  private final Tracer tracer;
  private final Propagator propagator;

  public TracePropagation(Tracer tracer, Propagator propagator) {
    this.tracer = tracer;
    this.propagator = propagator;
  }

  /**
   * Serializes the currently active trace context into a carrier map (W3C {@code traceparent} /
   * {@code tracestate} by default). Empty when nothing is being traced.
   */
  public Map<String, String> captureCurrent() {
    Map<String, String> carrier = new HashMap<>();
    TraceContext context = currentContext();
    if (context == null || propagator == null) {
      return carrier;
    }
    propagator.inject(context, carrier, Map::put);
    return carrier;
  }

  /** Trace id of the currently active span, or {@code null} when nothing is being traced. */
  public String currentTraceId() {
    TraceContext context = currentContext();
    return context == null ? null : context.traceId();
  }

  /**
   * Runs {@code action} inside a new span whose parent is the context previously captured by {@link
   * #captureCurrent()}. Anything instrumented that runs inside — an outgoing Kafka send, an HTTP
   * call — is then recorded as a child of the original request.
   */
  public <T> T continueTrace(Map<String, String> carrier, String spanName, Supplier<T> action) {
    if (tracer == null || propagator == null) {
      return action.get();
    }
    Map<String, String> safeCarrier = carrier == null ? Map.of() : carrier;
    Span span = propagator.extract(safeCarrier, Map::get).name(spanName).start();
    try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
      return action.get();
    } catch (RuntimeException ex) {
      span.error(ex);
      throw ex;
    } finally {
      span.end();
    }
  }

  private TraceContext currentContext() {
    return tracer == null ? null : tracer.currentTraceContext().context();
  }
}
