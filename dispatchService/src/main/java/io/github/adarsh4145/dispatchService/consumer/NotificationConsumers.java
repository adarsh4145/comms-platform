package io.github.adarsh4145.dispatchService.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.adarsh4145.core.event.NotificationCreatedEvent;
import io.github.adarsh4145.core.provider.SendRequest;
import io.github.adarsh4145.dispatchService.client.ProviderServiceClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class NotificationConsumers {

    private final ObjectMapper objectMapper;
    private final ProviderServiceClient providerServiceClient;


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

    private void handle(String priorityLabel, String payload) {
        log.info("[{}] Received notification event: {}", priorityLabel, payload);

        try {
            NotificationCreatedEvent event = objectMapper.readValue(payload, NotificationCreatedEvent.class);

            SendRequest sendRequest = new SendRequest(
                    SendRequest.Channel.EMAIL,
                    null,
                    event.recipient(),
                    "Notification",
                    event.message()
            );

            var response = providerServiceClient.send(sendRequest);
            log.info("[{}] Dispatched notification {} -> success={}", priorityLabel, event.requestId(), response.success());
        } catch (Exception ex) {
            log.error("[{}] Failed to process notification event: {}", priorityLabel, payload, ex);
        }

    }
}