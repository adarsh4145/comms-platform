package io.github.adarsh4145.upstreamSimulator.dto;

import lombok.*;

@Builder
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class NotificationRequest {
    private String recipient;
    private String message;
    private String priority;
}
