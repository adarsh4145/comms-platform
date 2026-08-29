package io.github.adarsh4145.cloudGateway.filter;

import java.nio.charset.StandardCharsets;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
public class ServiceUnavailableResponseFilter implements WebFilter {

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    return chain
        .filter(exchange)
        .then(
            Mono.defer(
                () -> {
                  ServerHttpResponse response = exchange.getResponse();
                  if (response.getStatusCode() != null
                      && response.getStatusCode().value() == HttpStatus.SERVICE_UNAVAILABLE.value()
                      && !response.isCommitted()) {

                    String body =
                        """
                                {"error":"service_unavailable","message":"The requested service is starting up or currently unreachable. Please retry shortly."}
                                """;
                    DataBuffer buffer =
                        response.bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8));
                    response.getHeaders().add("Content-Type", "application/json");
                    return response.writeWith(Mono.just(buffer));
                  }
                  return Mono.empty();
                }));
  }
}
