package io.github.adarsh4145.core.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class CommsBaseEvent {
  private String eventId;
  private String eventType;
  private Instant createdAt;
  private String sourceIdentifier;
  private String traceId;
}
