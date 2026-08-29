package io.github.adarsh4145.ingestionService.controller;

import io.github.adarsh4145.ingestionService.domain.NotificationRequest;
import io.github.adarsh4145.ingestionService.pojo.CreateNotificationRequest;
import io.github.adarsh4145.ingestionService.repository.NotificationRequestRepository;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Instant;

@RestController
@RequiredArgsConstructor
public class NotificationRequestController {

    private final NotificationRequestRepository repository;

    @PostMapping("/notifications")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<NotificationRequest> createNotification(@RequestBody CreateNotificationRequest request) {
        NotificationRequest toSave = NotificationRequest.builder()
                .recipient(request.recipient())
                .message(request.message())
                .priority(request.priority())
                .status(NotificationRequest.Status.RECEIVED)
                .createdAt(Instant.now())
                .build();

        return repository.save(toSave);
    }

}