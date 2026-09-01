package io.github.adarsh4145.core.event;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * The single mapper for everything that crosses Kafka.
 *
 * <p>Event payloads are a contract between services, so their encoding is pinned here rather than
 * left to whichever auto-configured {@code ObjectMapper} happens to win injection in a given
 * service — a difference that shows up only at runtime, on the far side of a topic.
 */
public class EventSerde {

  private final ObjectMapper mapper =
      JsonMapper.builder()
          .addModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .build();

  public String toJson(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to serialize event payload", ex);
    }
  }

  public <T> T fromJson(String json, Class<T> type) {
    try {
      return mapper.readValue(json, type);
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to deserialize event payload as " + type, ex);
    }
  }
}
