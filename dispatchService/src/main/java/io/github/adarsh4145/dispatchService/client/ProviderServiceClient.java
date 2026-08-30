package io.github.adarsh4145.dispatchService.client;

import io.github.adarsh4145.core.provider.SendRequest;
import io.github.adarsh4145.core.provider.SendResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "providerService")
public interface ProviderServiceClient {

    @PostMapping("/provider/send")
    SendResponse send(@RequestBody SendRequest request);
}