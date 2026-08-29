package io.github.adarsh4145.ingestionService.kafka;

public final class NotificationTopics {

  private NotificationTopics() {}

  public static final String CRITICAL = "notification.critical";
  public static final String HIGH = "notification.high";
  public static final String MEDIUM = "notification.medium";
  public static final String LOW = "notification.low";

  public static String forPriority(
      io.github.adarsh4145.ingestionService.domain.NotificationRequest.Priority priority) {
    return switch (priority) {
      case CRITICAL -> CRITICAL;
      case HIGH -> HIGH;
      case MEDIUM -> MEDIUM;
      case LOW -> LOW;
    };
  }
}
