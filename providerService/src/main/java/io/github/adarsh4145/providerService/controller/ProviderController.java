package io.github.adarsh4145.providerService.controller;

import io.github.adarsh4145.core.provider.SendRequest;
import io.github.adarsh4145.core.provider.SendResponse;
import io.github.adarsh4145.providerService.service.ProviderSimulationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("provider")
public class ProviderController {

    private final ProviderSimulationService providerSimulationService;

    @PostMapping("/send")
    public Mono<SendResponse> send(@RequestBody SendRequest request) {
        return providerSimulationService.send(request);
    }
}